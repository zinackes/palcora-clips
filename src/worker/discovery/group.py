"""Regrouper les VOD découvertes en events : une ancre officielle, ses candidates.

Fonction pure : le cron lui passe les VOD récentes et le catalogue, et écrit
ce qu'elle rend dans `discovered_events` / `discovered_sources`. Rien ici ne
confirme quoi que ce soit : une candidate n'entre dans l'event qu'après la
confirmation audio (`confirm.py`).

Les règles, dans l'ordre où elles tranchent :

- **Une ancre par match.** Seule une VOD de chaîne officielle (`competition`)
  au jeu connu peut ancrer. Quand plusieurs chaînes officielles diffusent le
  même match (LEC, OTP, LES en EN/FR/ES), la chaîne anglaise ancre, sinon la
  plus ancienne ; les autres deviennent des candidates. Le fil montre alors un
  event avec ses POV dans chaque langue, pas trois fois le même match.
- **Chevauchement.** Au moins `MIN_OVERLAP_S` en commun avec l'ancre : moins,
  c'est un stream qui s'arrête pendant le pré-show.
- **Même jeu.** Le jeu vu en direct doit être celui de l'ancre. Inconnu, il ne
  passe que si le catalogue donne ce jeu à la chaîne : une confirmation audio
  coûte un téléchargement, on ne la paie pas au hasard.

Deux streamers sur le même jeu à la même heure, sans ancre, ne forment rien :
c'est le faux regroupement que l'ancre obligatoire existe pour empêcher.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Literal

from worker.discovery.segments import Segment, detect_teams

# Une partie de LoL dure au moins 20 min ; 15 laissent la marge du pré-show.
MIN_OVERLAP_S = 15 * 60
ANCHOR_LANGUAGE = "en"


@dataclass(frozen=True)
class CatalogChannel:
    """Ce que le regroupement lit de `catalog_channels`, par id Twitch."""

    user_id: str
    kind: Literal["competition", "team", "creator"]
    language: str
    game_ids: tuple[str, ...]
    team: str | None = None


@dataclass(frozen=True)
class Vod:
    """Une ligne de `discovered_vods`, réduite à ce qui décide du regroupement."""

    vod_id: str
    user_id: str
    started_at: datetime
    duration_s: int
    game_id: str | None
    title: str

    @property
    def ended_at(self) -> datetime:
        return self.started_at + timedelta(seconds=self.duration_s)


@dataclass(frozen=True)
class Group:
    anchor: Vod
    candidates: tuple[Vod, ...]
    # Les tags d'équipe du match, dans l'ordre de son titre.
    teams: tuple[str, ...]
    # Le match dans l'ancre (ADR 0008) : bornes en secondes et titre du
    # segment ; une fin nulle court jusqu'au bout, un titre nul est celui de l'ancre.
    start_s: int = 0
    end_s: int | None = None
    title: str | None = None
    # D'où vient le découpage (`Segment.split`) : la carte cite Liquipedia s'il
    # vient du calendrier.
    split: str | None = None
    match_id: str | None = None


def overlap_s(a: Vod, b: Vod) -> float:
    start = max(a.started_at, b.started_at)
    end = min(a.ended_at, b.ended_at)
    return max(0.0, (end - start).total_seconds())


def _plausible_game(vod: Vod, anchor: Vod, channel: CatalogChannel | None) -> bool:
    if vod.game_id is not None:
        return vod.game_id == anchor.game_id
    return channel is not None and anchor.game_id in channel.game_ids


def _match(anchor: Vod, segment: Segment) -> Vod:
    """Le segment de l'ancre, comme une VOD : ce qu'une candidate doit chevaucher."""
    end_s = segment.end_s if segment.end_s is not None else anchor.duration_s
    return replace(
        anchor,
        started_at=anchor.started_at + timedelta(seconds=segment.start_s),
        duration_s=max(0, end_s - segment.start_s),
    )


def group(
    vods: Sequence[Vod],
    channels: Mapping[str, CatalogChannel],
    segments: Mapping[str, Sequence[Segment]] | None = None,
    frozen_anchors: frozenset[str] = frozenset(),
) -> list[Group]:
    """Les events que forment ces VOD, un par match, triés par début de match.

    `segments` découpe une ancre en matchs (par `vod_id`) ; une ancre absente
    reste un seul event, titré et équipé par la VOD elle-même.
    """

    def is_official(vod: Vod) -> bool:
        channel = channels.get(vod.user_id)
        return channel is not None and channel.kind == "competition"

    def priority(vod: Vod) -> tuple[bool, bool, datetime, str]:
        return (
            vod.vod_id not in frozen_anchors,
            channels[vod.user_id].language != ANCHOR_LANGUAGE,
            vod.started_at,
            vod.vod_id,
        )

    def identities(vod: Vod) -> frozenset[str] | None:
        split = tuple((segments or {}).get(vod.vod_id, ()))
        if not split or any(segment.match_id is None for segment in split):
            return None
        return frozenset(segment.match_id for segment in split if segment.match_id is not None)

    tags = [c.team for c in channels.values() if c.kind == "team" and c.team is not None]
    officials = sorted(
        (v for v in vods if is_official(v) and v.game_id is not None),
        key=priority,
    )
    others = [v for v in vods if not is_official(v)]

    groups: list[Group] = []
    taken: set[str] = set()
    for anchor in officials:
        if anchor.vod_id in taken:
            continue
        taken.add(anchor.vod_id)
        # Les autres diffusions officielles du même match, dans d'autres langues.
        official_candidates = [
            v
            for v in officials
            if v.vod_id not in taken
            and v.game_id == anchor.game_id
            and overlap_s(v, anchor) >= MIN_OVERLAP_S
        ]
        mirrors = [
            v
            for v in official_candidates
            if v.vod_id not in frozen_anchors
            and identities(anchor) is not None
            and identities(v) == identities(anchor)
        ]
        taken.update(v.vod_id for v in mirrors)
        for segment in (segments or {}).get(anchor.vod_id) or [Segment(0, None, None, ())]:
            match = _match(anchor, segment)
            candidates = sorted(
                [
                    *(
                        v
                        for v in official_candidates
                        if overlap_s(v, match) >= MIN_OVERLAP_S
                        and (
                            v not in mirrors
                            or (
                                segment.match_id is not None
                                and any(
                                    mirror.match_id == segment.match_id
                                    for mirror in (segments or {}).get(v.vod_id, ())
                                )
                            )
                        )
                    ),
                    *(
                        v
                        for v in others
                        if overlap_s(v, match) >= MIN_OVERLAP_S
                        and _plausible_game(v, anchor, channels.get(v.user_id))
                    ),
                ],
                key=lambda v: (v.started_at, v.vod_id),
            )
            teams = segment.teams if segment.title else detect_teams(anchor.title, tags)
            groups.append(
                Group(
                    anchor,
                    tuple(candidates),
                    teams,
                    start_s=segment.start_s,
                    end_s=segment.end_s,
                    title=segment.title,
                    split=segment.split,
                    match_id=segment.match_id,
                )
            )
    return sorted(groups, key=lambda g: (g.anchor.started_at, g.anchor.vod_id, g.start_s))
