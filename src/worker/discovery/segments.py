"""Découper une journée de broadcast en matchs, par ses titres (ADR 0008).

Les chaînes officielles changent de titre à chaque match (« KC vs VIT |
2026 LEC Summer Split… », « Team Vitality vs Virtus.pro | Group D Round 1 |
RLCS 2026 »). Le cron note ces titres ; cette fonction pure en tire les
segments de la VOD d'ancre, un par match :

- **un titre de match** nomme deux adversaires (« vs ») ; le pré-show, le
  post-show ou une pause n'en nomment pas, et ne forment pas de segment ;
- **un match est sa paire d'adversaires** : un libellé retouché en cours de
  match (« Week 8 » → « Week 8 Day 1 ») reste le même match, et un match qui
  reprend après une pause technique aussi ;
- **un match court jusqu'au titre suivant**, pause comprise : la production
  affiche le match suivant dès la fin du précédent, pas au premier game ;
- **moins de deux matchs distincts, un seul segment** — la VOD entière. Un
  titre fixe (`valorant_fr` nomme ses deux matchs dans un titre, l'EWC n'en
  nomme aucun) ne se découpe pas : jamais de découpage inventé.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

_VERSUS = re.compile(r"\bvs\.?(?=\s|$)", re.IGNORECASE)
# Ce qui sépare les adversaires du reste d'un titre de broadcast.
_SEPARATORS = re.compile(r"[|\[\]–—:/]| - ")


def detect_teams(title: str, tags: Iterable[str]) -> tuple[str, ...]:
    """Les tags présents en mots entiers dans le titre : « KC » oui, « KCB » non."""
    ordered = sorted(set(tags), key=len, reverse=True)
    if not ordered:
        return ()
    pattern = re.compile(
        r"(?<![A-Za-z0-9])(" + "|".join(map(re.escape, ordered)) + r")(?![A-Za-z0-9])"
    )
    return tuple(dict.fromkeys(match.group(1) for match in pattern.finditer(title)))


@dataclass(frozen=True)
class TitleObservation:
    observed_at: datetime
    title: str


@dataclass(frozen=True)
class Segment:
    """Un match dans la VOD d'ancre, en secondes ; une fin nulle court jusqu'au bout."""

    start_s: int
    end_s: int | None
    title: str | None
    teams: tuple[str, ...]
    # D'où vient le découpage : les titres de la chaîne, ou le calendrier
    # Liquipedia, que la carte doit alors citer (ADR 0009). Nul : pas de découpage.
    split: Literal["titles", "calendar"] | None = None
    match_id: str | None = None


# Des mots courts qui ne sont pas des sigles : « the mongolz » n'est pas
# « THE Mongolz ». Même liste que `teamName` du web.
_NOT_ACRONYMS = frozenset({"the", "of", "and", "de", "du", "des", "la", "le", "les", "et"})


def team_name(name: str | None) -> str:
    """« karmine corp » → « Karmine Corp » : Liquipedia écrit en minuscules, et un
    mot de trois lettres ou moins est un sigle, sauf un article ou une
    conjonction (`teamName` du web)."""
    if not name:
        return "À déterminer"
    return " ".join(
        word.upper()
        if len(word) <= 3 and word.lower() not in _NOT_ACRONYMS
        else word[:1].upper() + word[1:]
        for word in name.split(" ")
    )


def match_key(title: str) -> str | None:
    """La paire d'adversaires d'un titre (« kc vs vit »), ou None s'il n'en nomme pas."""
    found = _VERSUS.search(title)
    if found is None:
        return None
    left = _SEPARATORS.split(title[: found.start()])[-1]
    right = _SEPARATORS.split(title[found.end() :])[0]
    left, right = (" ".join(side.lower().split()) for side in (left, right))
    return f"{left} vs {right}" if left and right else None


@dataclass
class _Run:
    """Des observations consécutives du même match (ou d'un même hors-match)."""

    key: str | None
    start_s: int
    title: str


@dataclass
class _Open:
    key: str
    start_s: int
    title: str
    last_run: int


def segments(
    started_at: datetime,
    observations: Sequence[TitleObservation],
    team_tags: Iterable[str],
) -> list[Segment]:
    """Les matchs de la VOD commencée à `started_at`, dans l'ordre."""
    tags = tuple(team_tags)
    runs: list[_Run] = []
    for seen in sorted(observations, key=lambda item: item.observed_at):
        offset = max(0, int((seen.observed_at - started_at).total_seconds()))
        key = match_key(seen.title)
        if runs and runs[-1].key == key:
            runs[-1].title = seen.title  # le libellé le plus récent fait foi
        else:
            runs.append(_Run(key=key, start_s=offset, title=seen.title))

    if len({run.key for run in runs if run.key is not None}) < 2:
        # Aucun titre ne nomme de match (pré-show seul, titre de journée) : le
        # segment n'a pas de titre, et celui de la VOD d'ancre fera foi.
        matches = [run for run in runs if run.key is not None]
        title = matches[-1].title if matches else None
        return [Segment(0, None, title, detect_teams(title, tags) if title else ())]

    opened: list[_Open] = []
    for index, run in enumerate(runs):
        if run.key is None:
            continue
        previous = opened[-1] if opened else None
        # Le même match, revenu après une pause : on prolonge.
        if (
            previous is not None
            and previous.key == run.key
            and all(between.key is None for between in runs[previous.last_run + 1 : index])
        ):
            previous.last_run, previous.title = index, run.title
            continue
        # Rien avant lui : le match était déjà là quand le live a commencé.
        start = 0 if index == 0 else run.start_s
        opened.append(_Open(key=run.key, start_s=start, title=run.title, last_run=index))

    result: list[Segment] = []
    for match in opened:
        after = match.last_run + 1
        end = runs[after].start_s if after < len(runs) else None
        result.append(
            Segment(match.start_s, end, match.title, detect_teams(match.title, tags), "titles")
        )
    return result


@dataclass(frozen=True)
class PlannedMatch:
    """Un match du calendrier Liquipedia (ADR 0009), à son heure prévue."""

    scheduled_at: datetime
    team_a: str | None
    team_b: str | None
    teams: tuple[str, ...]
    competition: str
    match_id: str | None = None
    page: tuple[str, str] | None = None


def segments_from_schedule(
    started_at: datetime, duration_s: int, planned: Sequence[PlannedMatch]
) -> list[Segment] | None:
    """Les matchs d'une VOD d'après le calendrier, quand ses titres ne suffisent pas.

    Seulement s'il prévoit au moins deux matchs pendant la VOD, et tous de la
    même compétition : sinon rien, plutôt qu'un découpage deviné. L'heure est
    prévue, pas réelle — un match qui déborde décale le suivant.
    """
    inside = sorted(
        (
            match
            for match in planned
            if 0 <= (match.scheduled_at - started_at).total_seconds() < duration_s
        ),
        key=lambda match: match.scheduled_at,
    )
    if (
        len({match.page for match in inside}) > 1
        or len({match.competition for match in inside}) > 1
    ):
        return None
    # Deux matchs à la même heure (en parallèle, ou heures provisoires) : la VOD
    # ne dit pas lequel elle montre. Un créneau, un segment, jamais un segment
    # vide que `discovered_events` refuserait.
    slots: dict[int, list[PlannedMatch]] = {}
    for match in inside:
        slots.setdefault(int((match.scheduled_at - started_at).total_seconds()), []).append(match)
    if len(slots) < 2:
        return None
    starts = sorted(slots)
    return [
        Segment(
            start,
            starts[index + 1] if index + 1 < len(starts) else None,
            " / ".join(f"{team_name(m.team_a)} vs {team_name(m.team_b)}" for m in slots[start])
            + f" | {slots[start][0].competition}",
            tuple(team for m in slots[start] for team in m.teams),
            "calendar",
            slots[start][0].match_id if len(slots[start]) == 1 else None,
        )
        for index, start in enumerate(starts)
    ]
