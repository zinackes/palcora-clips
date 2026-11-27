"""Lire une playlist HLS et en tirer les segments d'une fenêtre.

`--download-sections` confie la découpe à ffmpeg, qui ouvre la playlist
lui-même. Deux cas lui échappent, tous deux mesurés le 2026-09-20 :

- au-delà d'un jour de VOD il ne rend plus de fenêtre (24,2 h répond en 3,1 s,
  45,4 h ne répond pas en 200 s, yt-dlp #12511) — or une chaîne qui diffuse en
  boucle produit exactement ces VOD-là, et c'est souvent un co-streamer ;
- sur une playlist de direct il démarre trois segments avant la fin et suit le
  flux sans s'arrêter.

Une playlist dit pourtant tout ce qu'il faut : la durée de chaque segment, donc
l'instant où il commence. Les cumuler donne la fenêtre sans rien demander à
ffmpeg, et rend le budget de temps réel puisque chaque requête est la nôtre.
Un direct se lit comme une VOD close, à l'`EXT-X-ENDLIST` près.

Twitch sert du fMP4 : le segment d'initialisation (`EXT-X-MAP`) porte les
en-têtes, et sans lui ffmpeg répond « no tfhd was found ». Il ouvre donc le
fichier.
"""

from __future__ import annotations

import math
import os
import tempfile
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import BinaryIO
from urllib.parse import urljoin, urlsplit

import httpx

# Twitch coupe le son a posteriori, par blocs de 30 min, et renomme le segment
# en `-muted`. L'original reste servi un moment : le prendre tant qu'il est là
# évite un trou dans ce qu'on écoute.
MUTED_SUFFIX = "-muted"
SEGMENT_TIMEOUT_S = 30.0
MAX_PLAYLIST_BYTES = 8 * 1024 * 1024
MAX_WINDOW_BYTES = 512 * 1024 * 1024
# Twitch media fragments are about ten seconds; 32 MiB leaves room even for
# high-bitrate video while stopping one unexpectedly large response promptly.
MAX_SEGMENT_BYTES = 32 * 1024 * 1024
MAX_WINDOW_SEGMENTS = 360
# Un segment audio Twitch pèse ~270 Ko : tiré seul, c'est la latence de la
# requête qui fixe le débit. Mesuré le 2026-11-14 sur des fenêtres de 10 min
# jamais tirées (CDN froid) d'une VOD de humzh : 13-14x le temps réel un par
# un, 79-80x à huit en vol, soit 6x plus vite.
SEGMENT_WORKERS = 8
MAX_REDIRECTS = 3
# Helix donne la durée d'une VOD à la seconde près ; la playlist terminée peut
# finir un peu avant (0,27 s sur riotgames, Worlds 2026). Mesuré sur quatre VOD :
# l'écart reste sous la seconde, dans les deux sens.
END_SLACK_S = 1.0


class UnreadablePlaylist(RuntimeError):
    """La playlist ne porte aucun segment : format inattendu ou réponse d'erreur."""


class WindowOutOfRange(RuntimeError):
    """La fenêtre demandée tombe hors de ce que la playlist couvre."""


class SegmentUnavailable(RuntimeError):
    """Un segment de la fenêtre n'a pas pu être tiré."""


class SegmentHTTPError(SegmentUnavailable):
    """Un serveur HLS a refusé un fragment avec ce statut HTTP."""

    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(f"fragment HLS indisponible (HTTP {status_code})")


class WindowTimeout(TimeoutError):
    """La fenêtre a dépassé son budget de temps."""


@dataclass(frozen=True)
class Segment:
    start_s: float
    duration_s: float
    uri: str

    @property
    def end_s(self) -> float:
        return self.start_s + self.duration_s


@dataclass(frozen=True)
class Playlist:
    segments: tuple[Segment, ...]
    init_uri: str | None
    # Un direct n'a pas d'`EXT-X-ENDLIST` ; ses segments sont là quand même.
    finished: bool
    resolved_url: str | None = None

    @property
    def duration_s(self) -> float:
        return self.segments[-1].end_s if self.segments else 0.0


def parse_playlist(text: str) -> Playlist:
    """Les segments d'une playlist média, chacun avec l'instant où il commence."""
    lines = text.splitlines()
    segments: list[Segment] = []
    init_uri: str | None = None
    elapsed = 0.0
    pending: float | None = None
    if not lines or lines[0].strip() != "#EXTM3U":
        raise UnreadablePlaylist("en-tête HLS absent")
    for line in lines[1:]:
        line = line.strip()
        if line.startswith("#EXT-X-KEY:"):
            if line != "#EXT-X-KEY:METHOD=NONE":
                raise UnreadablePlaylist("chiffrement HLS non pris en charge")
            continue
        if line.startswith(("#EXT-X-BYTERANGE:", "#EXT-X-GAP", "#EXT-X-I-FRAMES-ONLY")):
            raise UnreadablePlaylist("fragment HLS non pris en charge")
        if line.startswith("#EXT-X-MAP:"):
            if 'URI="' not in line or "BYTERANGE=" in line:
                raise UnreadablePlaylist("initialisation HLS non prise en charge")
            uri = line.split('URI="', 1)[1].split('"', 1)[0]
            if not uri or (init_uri is not None and uri != init_uri):
                raise UnreadablePlaylist("initialisation HLS changeante")
            init_uri = uri
            continue
        if line.startswith("#EXTINF:"):
            if pending is not None:
                raise UnreadablePlaylist("segment HLS sans URI")
            try:
                pending = float(line.removeprefix("#EXTINF:").split(",", 1)[0])
            except ValueError as exc:
                raise UnreadablePlaylist("durée HLS invalide") from exc
            if not math.isfinite(pending) or pending <= 0 or not math.isfinite(elapsed + pending):
                raise UnreadablePlaylist("durée HLS invalide")
            continue
        if not line or line.startswith("#"):
            continue
        if pending is None:
            raise UnreadablePlaylist("URI HLS sans durée")
        segments.append(Segment(start_s=elapsed, duration_s=pending, uri=line))
        elapsed += pending
        pending = None
    if pending is not None or not segments:
        raise UnreadablePlaylist(f"aucun segment dans {len(lines)} ligne(s) de playlist")
    return Playlist(
        segments=tuple(segments),
        init_uri=init_uri,
        finished="#EXT-X-ENDLIST" in text,
    )


def covering(playlist: Playlist, start_s: float, end_s: float) -> tuple[Segment, ...]:
    """Les segments que la fenêtre touche, dans l'ordre.

    Une playlist terminée accepte une fin jusqu'à `END_SLACK_S` au-delà de sa
    durée : c'est l'arrondi de Helix, et le fichier rendu s'arrête alors avec la VOD.
    """
    slack = END_SLACK_S if playlist.finished else 0.0
    if (
        not all(math.isfinite(v) for v in (start_s, end_s))
        or start_s < 0
        or end_s <= start_s
        or end_s > playlist.duration_s + slack
    ):
        raise WindowOutOfRange("fenêtre hors de la durée couverte par la playlist")
    wanted = tuple(
        segment
        for segment in playlist.segments
        if segment.start_s < end_s and segment.end_s > start_s
    )
    if not wanted:
        raise WindowOutOfRange(
            f"fenêtre {start_s:.0f}-{end_s:.0f} s hors des "
            f"{playlist.duration_s:.0f} s de la playlist"
        )
    return wanted


def _safe_url(base: str, uri: str) -> str:
    """Aucune URL secondaire ne peut quitter l'hôte CDN choisi par yt-dlp."""
    try:
        target = urljoin(base, uri)
        parsed = urlsplit(target)
        origin = urlsplit(base)
        allowed = (
            parsed.scheme == "https"
            and parsed.hostname == origin.hostname
            and parsed.port in (None, 443)
            and parsed.username is None
            and parsed.password is None
            and bool(parsed.hostname)
        )
    except ValueError:
        allowed = False
    if not allowed:
        raise SegmentUnavailable("destination HLS non autorisée") from None
    return target


def _response_chunks(
    client: httpx.Client,
    base: str,
    uri: str,
    deadline: float | None,
    on_final_url: Callable[[str], None] | None = None,
) -> Iterator[bytes]:
    target = _safe_url(base, uri)
    for _ in range(MAX_REDIRECTS + 1):
        if deadline is not None and time.monotonic() >= deadline:
            raise WindowTimeout("fenêtre au-delà de son budget")
        timeout = (
            min(SEGMENT_TIMEOUT_S, max(0.001, deadline - time.monotonic()))
            if deadline is not None
            else SEGMENT_TIMEOUT_S
        )
        try:
            with client.stream(
                "GET",
                target,
                headers={"Accept-Encoding": "identity"},
                timeout=timeout,
                follow_redirects=False,
            ) as response:
                if response.status_code in (301, 302, 303, 307, 308):
                    location = response.headers.get("location")
                    if not location:
                        raise SegmentUnavailable("redirection HLS sans destination")
                    target = _safe_url(base, urljoin(target, location))
                    continue
                if response.status_code != 200:
                    raise SegmentHTTPError(response.status_code)
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise SegmentUnavailable("encodage HLS non pris en charge")
                if on_final_url is not None:
                    on_final_url(target)
                # Check every transport chunk: a fixed chunk_size would buffer
                # a slow drip feed before returning control to this deadline.
                # Raw bytes also avoid decompression before the callers' caps.
                for chunk in response.iter_raw():
                    if deadline is not None and time.monotonic() >= deadline:
                        raise WindowTimeout("fenêtre au-delà de son budget")
                    yield chunk
                if deadline is not None and time.monotonic() >= deadline:
                    raise WindowTimeout("fenêtre au-delà de son budget")
                return
        except httpx.HTTPError:
            raise SegmentUnavailable("requête HLS échouée") from None
    raise SegmentUnavailable("trop de redirections HLS")


def fetch_playlist(client: httpx.Client, url: str, *, deadline: float | None = None) -> Playlist:
    data = bytearray()
    final_url = url

    def remember(target: str) -> None:
        nonlocal final_url
        final_url = target

    for chunk in _response_chunks(client, url, url, deadline, remember):
        if len(data) + len(chunk) > MAX_PLAYLIST_BYTES:
            raise UnreadablePlaylist("playlist HLS trop grande")
        data.extend(chunk)
    try:
        parsed = parse_playlist(data.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise UnreadablePlaylist("encodage HLS invalide") from exc
    if deadline is not None and time.monotonic() >= deadline:
        raise WindowTimeout("playlist au-delà de son budget")
    return replace(parsed, resolved_url=final_url)


def _read_segment(
    client: httpx.Client,
    playlist_url: str,
    uri: str,
    deadline: float | None,
    stop: threading.Event | None = None,
) -> bytes:
    """Un segment entier en mémoire : au plus `MAX_SEGMENT_BYTES`, l'original avant le muet."""
    candidates = [uri]
    if MUTED_SUFFIX in uri:
        candidates.insert(0, uri.replace(MUTED_SUFFIX, "", 1))
    for candidate in candidates:
        data = bytearray()
        try:
            for chunk in _response_chunks(client, playlist_url, candidate, deadline):
                if stop is not None and stop.is_set():
                    raise SegmentUnavailable("fenêtre abandonnée")
                data.extend(chunk)
                if len(data) > MAX_SEGMENT_BYTES:
                    raise SegmentUnavailable("fragment HLS trop grand")
            return bytes(data)
        except SegmentHTTPError as exc:
            if candidate == candidates[-1] or exc.status_code not in (403, 404):
                raise
    raise SegmentUnavailable("fragment HLS indisponible")


def _append(sink: BinaryIO, data: bytes, total: int) -> int:
    total += len(data)
    if total > MAX_WINDOW_BYTES:
        raise SegmentUnavailable("fenêtre HLS trop grande")
    sink.write(data)
    return total


def fetch_window(
    client: httpx.Client,
    playlist_url: str,
    playlist: Playlist,
    start_s: float,
    end_s: float,
    out: Path,
    *,
    deadline: float | None = None,
    workers: int = SEGMENT_WORKERS,
) -> float:
    """Écrit l'init et les segments de la fenêtre dans `out`.

    Rend l'instant où le fichier commence dans la VOD : les bornes d'un segment
    ne tombent pas sur celles de la fenêtre, l'appelant recoupe.

    `workers` segments sont en vol à la fois et s'écrivent dans l'ordre de la
    playlist ; la mémoire reste bornée à `workers` segments. Le premier échec
    arrête les autres et ne laisse aucun fichier.
    """
    if workers < 1:
        raise ValueError("workers doit valoir au moins 1")
    wanted = covering(playlist, start_s, end_s)
    if len(wanted) > MAX_WINDOW_SEGMENTS:
        raise SegmentUnavailable("trop de fragments HLS dans la fenêtre")

    def check() -> None:
        if deadline is not None and time.monotonic() >= deadline:
            raise WindowTimeout(f"fenêtre {start_s:.0f}-{end_s:.0f} s au-delà de son budget")

    playlist_url = playlist.resolved_url or playlist_url
    # Validate every URL before opening a file or starting any request.
    for uri in ([playlist.init_uri] if playlist.init_uri else []) + [s.uri for s in wanted]:
        _safe_url(playlist_url, uri)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp: Path | None = None
    stop = threading.Event()
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="hls")
    try:
        with tempfile.NamedTemporaryFile(
            dir=out.parent, prefix=f".{out.name}.", delete=False
        ) as sink:
            tmp = Path(sink.name)
            total = 0
            if playlist.init_uri is not None:
                check()
                init = _read_segment(client, playlist_url, playlist.init_uri, deadline)
                total = _append(sink, init, total)
            in_flight: deque[Future[bytes]] = deque()
            queue = iter(wanted)
            while True:
                while len(in_flight) < workers and (segment := next(queue, None)) is not None:
                    check()
                    in_flight.append(
                        pool.submit(
                            _read_segment, client, playlist_url, segment.uri, deadline, stop
                        )
                    )
                if not in_flight:
                    break
                total = _append(sink, in_flight.popleft().result(), total)
        check()
        os.replace(tmp, out)
    finally:
        stop.set()
        pool.shutdown(wait=True, cancel_futures=True)
        if tmp is not None:
            tmp.unlink(missing_ok=True)
    return wanted[0].start_s
