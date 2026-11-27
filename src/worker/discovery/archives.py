"""Ce que le cron de découverte demande à Helix : les archives, et le direct.

`/videos` ne rend pas la catégorie d'une archive, et ne filtre pas à la fois
par chaîne et par jeu. Le jeu d'une VOD vient donc d'ailleurs :

- du **direct** : `/streams` donne la catégorie des chaînes en live, et une
  archive porte le `stream_id` du live qui l'a produite. Le cron passe toutes
  les 15 min, pendant que l'archive grandit encore ;
- du **catalogue**, à défaut : une chaîne qui n'y a qu'un jeu le garde. C'est
  le cas de toutes les chaînes officielles, donc de toutes les ancres.

Sinon le jeu reste inconnu (`discovered_vods.game_id` nul) : un follow hors
catalogue découvert après la fin de son live.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from worker.ingest.discover import HELIX_STREAMS_URL, PAGE_SIZE, collect_videos
from worker.ingest.twitch import TwitchClient, _parse_created_at, parse_helix_duration

# Une chaîne produit une archive par live : deux pages couvrent largement la
# fenêtre du cron. Au-delà, `list_archives` le dit plutôt que de le taire.
ARCHIVE_PAGES = 2
# Ce que `discovered_vods` accepte : ISO 639-1, ou les deux valeurs propres à Twitch.
_LANGUAGE_RE = re.compile(r"^([a-z]{2}|other|asl)$")
# La vignette d'une VOD : seulement le CDN d'images de Twitch (ADR 0010). Le
# gabarit `%{width}x%{height}` reste dans l'URL, le site choisit la taille.
_THUMBNAIL_PREFIX = "https://static-cdn.jtvnw.net/"


@dataclass(frozen=True)
class Archive:
    """Une archive Twitch, prête pour `discovered_vods`. `started_at` est UTC aware."""

    vod_id: str
    user_id: str
    login: str
    title: str
    url: str
    started_at: datetime
    duration_s: int
    stream_id: str | None
    language: str
    # Nulle pendant le live (Helix rend `""`) ou hors du CDN Twitch.
    thumbnail_url: str | None = None


def thumbnail_url(value: object) -> str | None:
    text = str(value or "")
    return text if text.startswith(_THUMBNAIL_PREFIX) else None


def _archive(entry: Mapping[str, object]) -> Archive:
    language = str(entry.get("language") or "")
    stream_id = entry.get("stream_id")
    return Archive(
        vod_id=str(entry["id"]),
        user_id=str(entry["user_id"]),
        login=str(entry["user_login"]),
        title=str(entry.get("title") or ""),
        url=str(entry["url"]),
        started_at=_parse_created_at(str(entry["created_at"])),
        duration_s=parse_helix_duration(str(entry["duration"])),
        stream_id=str(stream_id) if stream_id else None,
        language=language if _LANGUAGE_RE.fullmatch(language) else "other",
        thumbnail_url=thumbnail_url(entry.get("thumbnail_url")),
    )


def list_archives(
    client: TwitchClient,
    user_id: str,
    since: datetime,
    *,
    max_pages: int = ARCHIVE_PAGES,
) -> tuple[list[Archive], bool]:
    """Les archives de la chaîne qui chevauchent `[since, maintenant]`, et la troncature.

    Une archive commencée avant `since` reste si elle finit après : un live de
    douze heures chevauche la fenêtre même s'il a commencé avant.
    """
    entries, truncated = collect_videos(
        client, user_id=user_id, stop_before=since, max_pages=max_pages
    )
    archives = [_archive(entry) for entry in entries]
    overlapping = [
        archive
        for archive in archives
        if archive.started_at + timedelta(seconds=archive.duration_s) >= since
    ]
    return overlapping, truncated


@dataclass(frozen=True)
class LiveStream:
    """Un live en cours, tel que `/streams` le décrit."""

    stream_id: str
    game_id: str | None
    title: str
    # Le login sert à suivre le live (`twitch.tv/<login>`), l'audience à choisir
    # qui enregistrer d'abord (`recordings.py`). Absents de la réponse, ils
    # restent vides : rien n'enregistre une chaîne sans login.
    login: str = ""
    viewers: int = 0


@dataclass(frozen=True)
class Live:
    """Ce que `/streams` dit des chaînes en direct parmi celles demandées."""

    user_ids: frozenset[str]
    # `stream_id` → catégorie ; un live sans catégorie n'y figure pas.
    games: dict[str, str]
    # `user_id` → le live en cours : son titre sert à découper une journée de
    # broadcast en matchs (ADR 0008).
    streams: dict[str, LiveStream]


def live_streams(client: TwitchClient, user_ids: Sequence[str]) -> Live:
    """Qui est en direct, et sur quel jeu.

    Le cron en tire deux choses : le jeu des archives en cours, et les chaînes
    à ne pas encore confirmer (une VOD en direct n'est pas extractible).
    `/streams` attend des `user_id` répétés, 100 par appel ; avec `first=100`,
    une page suffit à chaque lot.
    """
    live: set[str] = set()
    games: dict[str, str] = {}
    streams: dict[str, LiveStream] = {}
    for start in range(0, len(user_ids), PAGE_SIZE):
        chunk = list(user_ids[start : start + PAGE_SIZE])
        payload = client.get(
            HELIX_STREAMS_URL, {"first": PAGE_SIZE, "type": "live", "user_id": chunk}
        )
        for entry in payload.get("data", []) or []:
            if not isinstance(entry, dict) or not entry.get("user_id"):
                continue
            user_id = str(entry["user_id"])
            live.add(user_id)
            if entry.get("id") and entry.get("game_id"):
                games[str(entry["id"])] = str(entry["game_id"])
            if entry.get("id"):
                streams[user_id] = LiveStream(
                    stream_id=str(entry["id"]),
                    game_id=str(entry["game_id"]) if entry.get("game_id") else None,
                    title=str(entry.get("title") or ""),
                    login=str(entry.get("user_login") or ""),
                    viewers=_viewers(entry.get("viewer_count")),
                )
    return Live(user_ids=frozenset(live), games=games, streams=streams)


def _viewers(value: object) -> int:
    """`viewer_count`, ou 0 s'il manque ou ne se lit pas : il ne fait que l'ordre."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return 0
    return value


def vod_game(
    archive: Archive,
    live: Mapping[str, str],
    catalog_games: Sequence[str] | None,
) -> str | None:
    """Le jeu de l'archive : vu en direct, sinon l'unique jeu de la chaîne au catalogue."""
    if archive.stream_id is not None and archive.stream_id in live:
        return live[archive.stream_id]
    if catalog_games is not None and len(catalog_games) == 1:
        return catalog_games[0]
    return None
