"""Qui enregistrer pendant qu'une chaîne officielle diffuse un match (ADR 0014).

Le cron de découverte sait déjà qui est en direct, et sur quel jeu. Ce module
en tire la liste des enregistrements à lancer ; il ne lance rien lui-même :
`modal_app.py` tient l'ensemble des enregistrements en cours et lance
`record_stream`, qui appelle `ingest/live_record.py`.

Les règles sont celles de l'ADR :

- **qui** : les chaînes `creator` et `team` du catalogue, en live sur le jeu
  d'une chaîne `competition` du catalogue en direct, et dans une langue suivie
  (`FOLLOWED_LANGUAGES`). Jamais un inconnu, jamais une chaîne officielle ;
- **combien** : au plus `MAX_PARALLEL` en même temps, les plus regardés
  d'abord, et jamais deux fois le même streamer ;
- **si** : rien sans `MULTIPOV_RECORD_LIVES=1` (point 6, comme l'ADR 0013).

Tout est pur : ni Helix, ni Modal, ni base.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Set
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from worker.discovery.archives import Live
from worker.discovery.group import CatalogChannel
from worker.ingest.download import NoPlaylist, playlist_url
from worker.ingest.live_record import STATE_SUFFIX
from worker.official_feeds import FOLLOWED_LANGUAGES

# ADR 0014, point 4. La durée (12 h) et le poids (2 Gio) d'un enregistrement
# sont les plafonds par défaut de `record_live`.
MAX_PARALLEL = 8
# Combien de temps une entrée « en cours » compte, faute d'avoir été retirée :
# un conteneur tué ne nettoie rien. 13 h, c'est le plafond de 12 h de
# `record_live` plus la marge du délai Modal de `record_stream`.
ACTIVE_FOR = timedelta(hours=13)
RECORD_ENV = "MULTIPOV_RECORD_LIVES"
# Un login Twitch, tel que le catalogue l'accepte : il entre tel quel dans une
# clé R2 et dans une URL, rien d'autre n'y passe.
_LOGIN = re.compile(r"[a-z0-9_]{2,25}\Z")


@dataclass(frozen=True)
class RecordingStart:
    """Un enregistrement à lancer, et pourquoi : `official` diffuse `game_id`."""

    user_id: str
    login: str
    game_id: str
    official: str
    viewers: int

    @property
    def reason(self) -> str:
        return f"{self.official} en direct sur {self.game_id}"


def enabled(environ: Mapping[str, str]) -> bool:
    """Seul « 1 » allume l'enregistrement : absente ou autre, la variable l'éteint."""
    return environ.get(RECORD_ENV) == "1"


def to_record(
    live: Live,
    catalog: Mapping[str, CatalogChannel],
    active: Set[str],
    *,
    cap: int = MAX_PARALLEL,
) -> list[RecordingStart]:
    """Les co-streamers à enregistrer maintenant, les plus regardés d'abord.

    `catalog` est indexé par `user_id`, `active` porte les logins déjà en cours.
    Une chaîne officielle ne diffuse un match que sur un de ses jeux au
    catalogue : en « Just Chatting », elle n'entraîne personne. À audience
    égale, le login départage : l'ordre ne dépend pas de celui de Helix.
    """
    officials: dict[str, str] = {}  # jeu → login de la chaîne officielle
    for user_id, stream in sorted(live.streams.items(), key=lambda item: item[1].login):
        channel = catalog.get(user_id)
        if (
            channel is not None
            and channel.kind == "competition"
            and stream.game_id is not None
            and stream.game_id in channel.game_ids
        ):
            officials.setdefault(stream.game_id, stream.login)

    starts: list[RecordingStart] = []
    for user_id, stream in live.streams.items():
        channel = catalog.get(user_id)
        if (
            channel is None
            or channel.kind not in ("creator", "team")
            or channel.language not in FOLLOWED_LANGUAGES
            or stream.game_id is None
            or stream.game_id not in officials
            or not _LOGIN.fullmatch(stream.login)
            or stream.login in active
        ):
            continue
        starts.append(
            RecordingStart(
                user_id=user_id,
                login=stream.login,
                game_id=stream.game_id,
                official=officials[stream.game_id],
                viewers=stream.viewers,
            )
        )
    starts.sort(key=lambda start: (-start.viewers, start.login))
    return starts[: max(0, cap - len(active))]


def still_active(started: Mapping[str, datetime], now: datetime) -> set[str]:
    """Les logins dont l'enregistrement a commencé il y a moins de `ACTIVE_FOR`."""
    return {login for login, at in started.items() if now - at < ACTIVE_FOR}


def recording_key(login: str, started_at: datetime) -> str:
    """`raw/recordings/<login>/<début UTC>.mp4`, le début en ISO 8601 compact.

    Sous `raw/`, l'objet tombe sous la règle de `infra/r2-lifecycle.json` :
    purgé au bout de 7 jours, comme une fenêtre de VOD. C'est le délai de l'ADR
    pour un enregistrement qu'aucun match n'utilise ; garder 90 jours celui
    qu'un match utilise, c'est l'item 14 du plan (`discovery/purge.py`).

    Le fichier enchaîne les segments tels que Twitch les sert, comme les
    `.segments.mp4` de `download.py` : l'extension suit celle des fenêtres.
    """
    if not _LOGIN.fullmatch(login):
        raise ValueError("login Twitch invalide")
    if started_at.tzinfo is None:
        raise ValueError("début d'enregistrement sans fuseau")
    stamp = started_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"raw/recordings/{login}/{stamp}.mp4"


def state_key(key: str) -> str:
    """La clé de l'état de `record_live`, posé à côté de l'enregistrement."""
    return key + STATE_SUFFIX


def audio_playlist(info: Mapping[str, object]) -> str:
    """La playlist média de la seule variante audio du live.

    `playlist_url` se replie sur le premier format quand l'audio manque : bon
    pour une confirmation, pas ici, où l'ADR exclut la vidéo (point 1). Les
    formats autres que l'audio sont donc retirés avant, et la vérification
    d'hôte de `playlist_url` s'applique telle quelle. yt-dlp nomme la variante
    `Audio_Only` sur une VOD, `audio_only` sur un direct.
    """
    formats = info.get("formats")
    audio = [
        entry
        for entry in (formats if isinstance(formats, list) else [])
        if isinstance(entry, dict) and str(entry.get("format_id") or "").casefold() == "audio_only"
    ]
    if not audio:
        raise NoPlaylist("pas de variante audio dans le probe du direct")
    return playlist_url({"formats": [{**audio[0], "format_id": "Audio_Only"}]})
