"""Enregistrer l'audio d'un direct Twitch en suivant sa playlist.

Une VOD peut manquer après coup : réservée aux abonnés, supprimée, expirée.
Le seul moyen d'avoir le son d'un co-streamer, c'est alors de l'écouter
pendant qu'il diffuse. `hls.py` lit déjà une playlist de direct comme une VOD
close, à l'`EXT-X-ENDLIST` près ; ce module la relit à intervalle régulier et
ajoute au fichier chaque segment qu'il n'a pas encore vu.

Trois choses distinguent un direct d'une VOD :

- la playlist grandit, et Twitch fait tourner sa fenêtre : les plus vieux
  segments en sortent à mesure que les nouveaux arrivent. Un segment se
  reconnaît donc à son numéro — `EXT-X-MEDIA-SEQUENCE` plus son rang dans la
  liste (RFC 8216 §4.3.3.2) — jamais à sa place. `parse_playlist` n'expose pas
  ce numéro : on le lit ici, dans le même texte. L'URI du dernier segment
  écrit sert de contrôle : si son numéro porte une autre URI, la numérotation
  a changé et rien de ce qui suit n'est sûr ;
- la fin ne s'annonce pas toujours. `EXT-X-ENDLIST` arrive quand le live se
  termine proprement ; quand le flux est coupé, la playlist cesse simplement
  de grandir, puis répond 404. Les deux comptent comme une lecture sans
  nouveau segment, et `STALL_POLLS` lectures de suite arrêtent l'écoute ;
- un direct dure des heures et le conteneur peut mourir en route. Un petit
  état JSON, à côté du fichier, dit le dernier segment traité, l'instant où il
  commence et le nombre d'octets écrits. Il s'écrit d'un bloc (fichier
  temporaire puis `os.replace`) après chaque segment : une reprise tronque ce
  que l'état ne connaît pas encore — un segment écrit mais pas consigné — et
  repart du numéro suivant, sans doublon.

Les temps sont comptés depuis le premier segment enregistré, pas depuis le
début du live : c'est le recalage audio qui place le fichier, comme pour une
fenêtre de VOD.

Un segment qu'on n'a pas eu n'est pas oublié en silence. Sorti de la fenêtre
avant d'être lu, ou refusé par le serveur, il devient un trou : un intervalle
(début, fin) dans l'état. Le fichier, lui, enchaîne les segments présents ;
c'est l'état qui dit où le son manque.

Les segments se lisent un par un, sans les huit en vol de `fetch_window` : une
fois rattrapée la fenêtre de départ, le direct n'en produit qu'un toutes les
quelques secondes, et un seul segment se tire 13 à 14 fois plus vite que le
temps réel (mesure du 2026-11-14 dans `hls.py`).

Rien ici n'appelle ce module : le câblage (item 13) attend l'ADR 0014.
"""

from __future__ import annotations

import json
import math
import os
import tempfile
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from typing import BinaryIO, Literal

import httpx

from worker.ingest.hls import (
    MAX_PLAYLIST_BYTES,
    Playlist,
    Segment,
    SegmentUnavailable,
    UnreadablePlaylist,
    _read_segment,
    _response_chunks,
    parse_playlist,
)

# RFC 8216 §6.3.4 : relire une playlist de direct au plus tous les
# EXT-X-TARGETDURATION. Twitch annonce 2 s sur ses directs (6 s en
# faible latence désactivée) : 2 s suit le live sans rater de segment.
POLL_INTERVAL_S = 2.0
# Une coupure de réseau du streamer dure quelques secondes ; OBS se reconnecte
# en général sous 30 s. Quinze lectures vides de suite, soit 30 s à 2 s
# d'intervalle, font la différence entre un hoquet et un live terminé.
STALL_POLLS = 15
# Une journée de compétition longue (Worlds, LEC Finals) tient en 12 h. L'ADR
# 0014 fixera le vrai plafond ; celui-ci borne un appel oublié.
MAX_RECORD_S = 12 * 3600.0
# L'audio Twitch pèse ~270 Ko par 10 s, soit ~100 Mo par heure : 2 Gio couvrent
# `MAX_RECORD_S` avec une marge de 60 %, et arrêtent un flux anormalement lourd.
MAX_RECORD_BYTES = 2 * 1024 * 1024 * 1024
STATE_SUFFIX = ".state.json"
STATE_VERSION = 1

StopReason = Literal["ended", "stalled", "duration", "bytes"]


class LiveRecordError(RuntimeError):
    """L'enregistrement ne peut pas continuer sans risquer un doublon ou un trou muet."""


@dataclass
class LiveState:
    """Ce qui permet de reprendre : le dernier segment traité, écrit ou perdu."""

    last_sequence: int | None = None
    last_uri: str | None = None
    # Début et durée du dernier segment, en secondes depuis le premier enregistré.
    last_start_s: float = 0.0
    last_duration_s: float = 0.0
    bytes_written: int = 0
    init_uri: str | None = None
    # Le son qui manque au fichier, en intervalles (début, fin) fusionnés.
    holes: list[tuple[float, float]] = field(default_factory=list)

    @property
    def end_s(self) -> float:
        return self.last_start_s + self.last_duration_s


@dataclass(frozen=True)
class Record:
    reason: StopReason
    state: LiveState


@dataclass(frozen=True)
class _Poll:
    playlist: Playlist
    media_sequence: int
    base_url: str


def coverage(state: LiveState, *, expected_s: float, late_s: float = 0.0) -> float:
    """La part du direct que le fichier tient : le critère de la phase D.

    `expected_s` est la durée du direct (Helix `started_at` à sa fin) ;
    `late_s`, le retard du premier segment enregistré sur ce début. Ce retard,
    les trous de l'état et ce qui manque à la fin comptent comme du son absent ;
    ce qui dépasse la fin du direct ne compte pas. Les temps de l'état partent
    du premier segment, d'où le décalage de `late_s`.
    """
    if not math.isfinite(expected_s) or expected_s <= 0:
        raise ValueError("la durée du direct doit être un nombre positif de secondes")
    if not math.isfinite(late_s) or late_s < 0:
        raise ValueError("le retard du premier segment doit être positif ou nul")
    limit = expected_s - late_s
    if limit <= 0:
        return 0.0
    heard = min(state.end_s, limit)
    for start, end in state.holes:
        heard -= max(0.0, min(end, limit) - max(start, 0.0))
    return min(1.0, max(0.0, heard / expected_s))


def state_path(out: Path) -> Path:
    return out.with_name(out.name + STATE_SUFFIX)


def load_state(path: Path) -> LiveState | None:
    """L'état consigné, ou `None` s'il n'y en a pas encore."""
    try:
        raw: object = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise LiveRecordError("état d'enregistrement illisible") from exc
    if not isinstance(raw, dict) or raw.get("version") != STATE_VERSION:
        raise LiveRecordError("état d'enregistrement d'une autre version")
    try:
        state = LiveState(
            last_sequence=_optional_int(raw["last_sequence"]),
            last_uri=_optional_str(raw["last_uri"]),
            last_start_s=_number(raw["last_start_s"]),
            last_duration_s=_number(raw["last_duration_s"]),
            bytes_written=_int(raw["bytes_written"]),
            init_uri=_optional_str(raw["init_uri"]),
            holes=[_interval(hole) for hole in _list(raw["holes"])],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise LiveRecordError("état d'enregistrement invalide") from exc
    if state.bytes_written < 0 or (state.last_sequence is None) != (state.last_uri is None):
        raise LiveRecordError("état d'enregistrement invalide")
    return state


def _save_state(path: Path, state: LiveState) -> None:
    """D'un bloc : une mort en pleine écriture laisse l'ancien état intact."""
    payload = {"version": STATE_VERSION, **asdict(state)}
    tmp: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as sink:
            tmp = Path(sink.name)
            json.dump(payload, sink)
        os.replace(tmp, path)
        tmp = None
    finally:
        if tmp is not None:
            tmp.unlink(missing_ok=True)


def _media_sequence(text: str) -> int:
    """Le numéro du premier segment ; 0 quand la playlist n'en dit rien (RFC 8216)."""
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            value = line.removeprefix("#EXT-X-MEDIA-SEQUENCE:")
            if not value.isdigit():
                raise UnreadablePlaylist("numéro de séquence HLS invalide")
            return int(value)
    return 0


def _poll(client: httpx.Client, url: str) -> _Poll:
    """`fetch_playlist`, à ceci près qu'il garde le texte pour en lire le numéro de séquence.

    Même transport (`_response_chunks` : hôte vérifié, redirections bornées,
    pas de décompression) et même plafond d'octets.
    """
    data = bytearray()
    final_url = url

    def remember(target: str) -> None:
        nonlocal final_url
        final_url = target

    for chunk in _response_chunks(client, url, url, None, remember):
        if len(data) + len(chunk) > MAX_PLAYLIST_BYTES:
            raise UnreadablePlaylist("playlist HLS trop grande")
        data.extend(chunk)
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise UnreadablePlaylist("encodage HLS invalide") from exc
    playlist = parse_playlist(text)
    return _Poll(
        playlist=replace(playlist, resolved_url=final_url),
        media_sequence=_media_sequence(text),
        base_url=final_url,
    )


def _reconcile(out: Path, state: LiveState | None) -> LiveState:
    """Ramène le fichier à ce que l'état connaît, ou refuse s'il ne lui appartient pas."""
    size = out.stat().st_size if out.exists() else 0
    if state is None:
        if size:
            raise LiveRecordError("fichier existant sans état d'enregistrement")
        return LiveState()
    if size < state.bytes_written:
        raise LiveRecordError("fichier plus court que son état d'enregistrement")
    if size > state.bytes_written:
        # Un segment écrit, mort avant de le consigner : il sera relu.
        with out.open("r+b") as sink:
            sink.truncate(state.bytes_written)
    return state


def _add_hole(state: LiveState, start_s: float, end_s: float) -> None:
    if end_s <= start_s:
        return
    if state.holes and math.isclose(state.holes[-1][1], start_s, abs_tol=1e-6):
        state.holes[-1] = (state.holes[-1][0], end_s)
    else:
        state.holes.append((start_s, end_s))


def record_live(
    client: httpx.Client,
    playlist_url: str,
    out: Path,
    *,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
    poll_interval_s: float = POLL_INTERVAL_S,
    stall_polls: int = STALL_POLLS,
    max_duration_s: float = MAX_RECORD_S,
    max_bytes: int = MAX_RECORD_BYTES,
) -> Record:
    """Suit la playlist média d'un direct et ajoute à `out` chaque nouveau segment.

    S'arrête sur `EXT-X-ENDLIST`, après `stall_polls` lectures sans nouveau
    segment, ou avant le segment qui dépasserait `max_duration_s` (comptés
    depuis le premier segment enregistré) ou `max_bytes`. Reprend depuis
    l'état posé à côté de `out` s'il existe.

    `sleep` et `clock` sont injectables : l'attente entre deux lectures vaut
    `poll_interval_s` moins le temps qu'a pris la lecture.
    """
    if poll_interval_s < 0 or stall_polls < 1 or max_duration_s <= 0 or max_bytes < 1:
        raise ValueError("plafonds d'enregistrement invalides")
    out.parent.mkdir(parents=True, exist_ok=True)
    path = state_path(out)
    state = _reconcile(out, load_state(path))
    empty_polls = 0
    with out.open("ab") as sink:
        while True:
            started = clock()
            try:
                poll: _Poll | None = _poll(client, playlist_url)
            except (SegmentUnavailable, UnreadablePlaylist):
                # Une playlist qui répond 404 ou ne se lit plus : le flux est
                # peut-être coupé. Elle compte comme une lecture sans croissance.
                poll = None
            grew = False
            if poll is not None:
                stop, grew = _consume(client, poll, state, sink, path, max_duration_s, max_bytes)
                if stop is not None:
                    return Record(stop, state)
                if poll.playlist.finished:
                    return Record("ended", state)
            empty_polls = 0 if grew else empty_polls + 1
            if empty_polls >= stall_polls:
                return Record("stalled", state)
            sleep(max(0.0, poll_interval_s - (clock() - started)))


def _consume(
    client: httpx.Client,
    poll: _Poll,
    state: LiveState,
    sink: BinaryIO,
    path: Path,
    max_duration_s: float,
    max_bytes: int,
) -> tuple[StopReason | None, bool]:
    """Ajoute les segments que l'état ne connaît pas ; rend (arrêt, a grandi)."""
    playlist = poll.playlist
    numbered = [(poll.media_sequence + i, s) for i, s in enumerate(playlist.segments)]
    if state.last_sequence is not None:
        for sequence, segment in numbered:
            if sequence == state.last_sequence and segment.uri != state.last_uri:
                raise LiveRecordError("numérotation HLS changée en cours de direct")
    fresh = [
        (sequence, segment)
        for sequence, segment in numbered
        if state.last_sequence is None or sequence > state.last_sequence
    ]
    if not fresh:
        return None, False

    if state.bytes_written == 0 and state.last_sequence is None:
        if playlist.init_uri is not None:
            init = _read_segment(client, poll.base_url, playlist.init_uri, None)
            if len(init) > max_bytes:
                return "bytes", True
            _write(sink, init, state)
        state.init_uri = playlist.init_uri
    elif playlist.init_uri != state.init_uri:
        # Un autre segment d'initialisation : les segments suivants ne se
        # décodent plus derrière les premiers, le fichier ne peut que s'arrêter.
        raise LiveRecordError("initialisation HLS changée en cours de direct")

    cursor = state.end_s
    first_sequence = fresh[0][0]
    if state.last_sequence is not None and first_sequence > state.last_sequence + 1:
        # Sortis de la fenêtre avant d'être lus : leur durée est estimée sur
        # celle des segments présents, faute de mieux.
        missing = first_sequence - state.last_sequence - 1
        mean_s = playlist.duration_s / len(playlist.segments)
        _add_hole(state, cursor, cursor + missing * mean_s)
        cursor += missing * mean_s

    for sequence, segment in fresh:
        if cursor + segment.duration_s > max_duration_s:
            _save_state(path, state)
            return "duration", True
        try:
            data: bytes | None = _read_segment(client, poll.base_url, segment.uri, None)
        except SegmentUnavailable:
            data = None
        if data is not None and state.bytes_written + len(data) > max_bytes:
            _save_state(path, state)
            return "bytes", True
        if data is None:
            _add_hole(state, cursor, cursor + segment.duration_s)
        else:
            _write(sink, data, state)
        _advance(state, sequence, segment, cursor)
        _save_state(path, state)
        cursor += segment.duration_s
    return None, True


def _write(sink: BinaryIO, data: bytes, state: LiveState) -> None:
    sink.write(data)
    # L'état ne doit jamais compter des octets restés dans un tampon.
    sink.flush()
    state.bytes_written += len(data)


def _advance(state: LiveState, sequence: int, segment: Segment, start_s: float) -> None:
    state.last_sequence = sequence
    state.last_uri = segment.uri
    state.last_start_s = start_s
    state.last_duration_s = segment.duration_s


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError("nombre attendu")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("nombre fini attendu")
    return number


def _int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("entier attendu")
    return value


def _optional_int(value: object) -> int | None:
    return None if value is None else _int(value)


def _optional_str(value: object) -> str | None:
    if value is not None and not isinstance(value, str):
        raise TypeError("texte attendu")
    return value


def _list(value: object) -> list[object]:
    if not isinstance(value, list):
        raise TypeError("liste attendue")
    return value


def _interval(value: object) -> tuple[float, float]:
    pair = _list(value)
    if len(pair) != 2:
        raise ValueError("intervalle attendu")
    start, end = _number(pair[0]), _number(pair[1])
    if end <= start:
        raise ValueError("intervalle vide")
    return (start, end)
