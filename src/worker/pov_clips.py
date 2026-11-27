"""Les extraits gardés chez nous (docs/plans/moments-chez-nous.md).

Twitch efface une VOD au bout de 60 jours : pour chaque moment publié et
chacune de ses POV, on garde le passage de la VOD, avec son audio, dans R2
sous `clips/twitch/<vod>/<début>-<durée>.mp4`.

L'extrait appartient à la VOD, pas au moment : republier une analyse recrée
les moments (nouveaux ids), et un extrait rangé par moment serait perdu à la
première republication après l'expiration. Un moment retrouve ses extraits
par recouvrement (`covers`) ; la marge `CLIP_MARGIN_S` laisse de la place à
une recoupe.

Temps de la VOD = temps de l'ancre + écart des horloges + offset publié
(`discovery/publish.py`), comme le lecteur du site.

Chaque fenêtre a sa ligne dans `pov_clips` : `done` avec sa hauteur et son
poids, ou `failed` avec son nombre d'essais. Une VOD illisible n'est donc
pas relue à chaque passe.
"""

from __future__ import annotations

import json
import logging
import math
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterable, Mapping, Set
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from worker import moment_previews
from worker.limits import (
    CLIP_MARGIN_S,
    CLIP_MAX_ATTEMPTS,
    CLIP_MAX_S,
    CLIP_RUN_UP_S,
    CLIPS_PER_PASS,
)
from worker.storage import INFREQUENT, ClipWindow

log = logging.getLogger(__name__)

MAX_ATTEMPTS = CLIP_MAX_ATTEMPTS
# Lire jusqu'à 2 min de HLS en 1080p et les encoder sur 2 cœurs.
COMMAND_TIMEOUT_S = 420
# Une VOD a parfois un trou d'une seconde ou deux (segments perdus pendant le
# live) : l'extrait sort plus court que sa fenêtre, et la marge l'absorbe.
# Au-delà, la VOD finit avant le moment.
SHORT_BY_S = CLIP_MARGIN_S
Window = tuple[int, int]
"""Le début et la durée d'un extrait, en secondes entières de la VOD."""


class TooShort(RuntimeError):
    """La VOD finit avant la fin de la fenêtre : la POV n'a pas tout le moment."""


class VodGone(RuntimeError):
    """Twitch a effacé la VOD : plus rien à couper, ni maintenant ni plus tard."""


class Uploader(Protocol):
    def upload(self, path: Path, key: str, *, storage_class: str | None = None) -> None: ...


class Connection(Protocol):
    def execute(self, query: str, params: Mapping[str, object] | None = None) -> object: ...


@dataclass(frozen=True)
class Wanted:
    """Un moment vu d'une POV, en temps de sa VOD, sans marge."""

    vod_id: str
    start_s: float
    end_s: float


@dataclass(frozen=True)
class Made:
    path: Path
    height: int
    bytes: int
    duration_s: float


@dataclass
class Report:
    made: int = 0
    failed: int = 0
    # VOD que Twitch a effacées, marquées `expired` au passage.
    gone: int = 0


def pov_time(anchor_s: float, *, clock_gap_s: float, offset_ms: int) -> float:
    """L'instant de la VOD d'une POV pour un instant de l'ancre."""
    return anchor_s + clock_gap_s + offset_ms / 1000


def clip_window(start_s: float, end_s: float) -> Window:
    """La fenêtre entière qui couvre le moment et sa marge, bornée par `CLIP_MAX_S`."""
    if end_s <= 0 or end_s <= start_s:
        raise ValueError("moment hors de la VOD")
    begin = max(0, math.floor(start_s - CLIP_MARGIN_S))
    duration = min(math.ceil(end_s + CLIP_MARGIN_S) - begin, CLIP_MAX_S)
    return begin, duration


def clip_key(vod_id: str, start_s: int, duration_s: int) -> str:
    """La clé R2 de l'extrait d'une VOD Twitch ; tout autre identifiant est refusé."""
    if not vod_id.isascii() or not vod_id.isdigit():
        raise ValueError(f"VOD Twitch attendue : {vod_id!r}")
    return ClipWindow("twitch", vod_id, start_s, duration_s).key


def covers(clip: Window, start_s: float, end_s: float) -> bool:
    """L'extrait contient-il tout le passage [start_s, end_s] de sa VOD ?"""
    begin, duration = clip
    return begin <= start_s and end_s <= begin + duration


def missing(
    wanted: Iterable[Wanted],
    made: Mapping[str, Iterable[Window]],
    *,
    given_up: Mapping[str, Set[Window]] | None = None,
) -> dict[str, list[Window]]:
    """Les fenêtres à couper, par VOD : ni déjà faites, ni abandonnées.

    `wanted` arrive trié par début dans chaque VOD. Deux moments qui se
    chevauchent partagent un seul extrait tant qu'il tient dans `CLIP_MAX_S`
    (un cinquième des fichiers et un dixième du poids en moins, mesuré le
    26/11/2026).
    """
    todo: dict[str, list[Window]] = {}
    for item in wanted:
        try:
            window = clip_window(item.start_s, item.end_s)
        except ValueError:
            continue
        if any(covers(clip, item.start_s, item.end_s) for clip in made.get(item.vod_id, ())):
            continue
        chosen = todo.setdefault(item.vod_id, [])
        if chosen:
            begin, duration = chosen[-1]
            end = max(begin + duration, window[0] + window[1])
            if begin <= window[0] <= begin + duration and end - begin <= CLIP_MAX_S:
                chosen[-1] = (begin, end - begin)
                continue
        chosen.append(window)
    skipped = given_up or {}
    kept = {
        vod: [w for w in windows if w not in skipped.get(vod, ())] for vod, windows in todo.items()
    }
    return {vod: windows for vod, windows in kept.items() if windows}


def clip_command(
    source: str, start_s: float, duration_s: int, out: Path, preset: str = moment_previews.PRESET
) -> list[str]:
    # Les réglages vidéo de l'aperçu (1080p au plus, 30 i/s, débit plafonné),
    # plus le son du streamer : c'est tout ce qui restera de sa VOD.
    guard = ("-protocol_whitelist", "https,tls,tcp,crypto") if source.startswith("https://") else ()
    # Un saut dans une playlist HLS atterrit sur l'image clé qui SUIT l'instant
    # demandé (mesuré : jusqu'à 2,6 s de trou en tête). La lecture part donc
    # `run_up` secondes plus tôt, et la sortie coupe à l'instant exact.
    run_up = min(CLIP_RUN_UP_S, start_s)
    return [
        "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
        *guard, "-ss", str(start_s - run_up), "-i", source,
        "-ss", str(run_up), "-t", str(duration_s),
        "-vf", f"scale=-2:min(ih\\,{moment_previews.MAX_HEIGHT}),fps=30",
        *moment_previews.video_args(preset),
        "-c:a", "aac", "-b:a", "128k", "-ac", "2", "-ar", "48000",
        "-movflags", "+faststart",
        str(out),
    ]  # fmt: skip


def _probe(path: Path) -> tuple[int, float]:
    """La hauteur de l'image et la durée du fichier."""
    done = subprocess.run(  # noqa: S603 — liste d'arguments, jamais de shell
        [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=height:format=duration", "-of", "json", str(path),
        ],
        capture_output=True, text=True, timeout=60, check=True,
    )  # fmt: skip
    read = json.loads(done.stdout)
    if not read.get("streams"):
        # La fenêtre tombe dans un trou de la VOD : ffmpeg n'a rien eu à encoder.
        raise TooShort("aucune image lue")
    return int(read["streams"][0]["height"]), float(read["format"]["duration"])


Fetch = Callable[[str, float, float, Path], tuple[Path, float]]
"""Tire les segments d'une fenêtre : (playlist, début, fin, dossier) → (fichier, son début)."""


def fetch_segments(source: str, start_s: float, end_s: float, workdir: Path) -> tuple[Path, float]:
    """Les seuls segments HLS de la fenêtre, init fMP4 compris, dans un fichier local.

    ffmpeg ne sait pas sauter au milieu d'une VOD Twitch servie en fMP4 : il lit
    depuis le début (27/11/2026, runner GitHub : 20 min sans une image pour
    20 s à 1 h 40, « aucune image lue » ou délai dépassé sur 27 % des fenêtres).
    `ingest.hls` connaît l'init et les segments `-muted` ; c'est lui qui tire.
    """
    import httpx

    from worker.ingest import hls

    deadline = time.monotonic() + COMMAND_TIMEOUT_S
    out = workdir / "segments.mp4"
    with httpx.Client(follow_redirects=False) as client:
        playlist = hls.fetch_playlist(client, source, deadline=deadline)
        try:
            first = hls.fetch_window(
                client, source, playlist, start_s, end_s, out, deadline=deadline
            )
        except hls.WindowOutOfRange as error:
            raise TooShort(f"la VOD finit avant la fenêtre : {error}") from error
    return out, first


def cut(
    source: str,
    start_s: int,
    duration_s: int,
    workdir: Path,
    preset: str = moment_previews.PRESET,
    *,
    fetch: Fetch = fetch_segments,
) -> Made:
    """Coupe la fenêtre dans `workdir` ; refuse un extrait plus court que demandé.

    Une playlist distante est d'abord réduite aux segments de la fenêtre
    (`fetch`), puis coupée en local ; un fichier local se coupe tel quel.
    """
    out = workdir / f"{start_s}-{duration_s}.mp4"
    begin: float = start_s
    if source.startswith("https://"):
        local, first = fetch(
            source, max(0.0, start_s - CLIP_RUN_UP_S), start_s + duration_s, workdir
        )
        source, begin = str(local), round(start_s - first, 3)
    done = subprocess.run(  # noqa: S603 — liste d'arguments, jamais de shell
        clip_command(source, begin, duration_s, out, preset),
        capture_output=True, text=True, timeout=COMMAND_TIMEOUT_S, check=False,
    )  # fmt: skip
    if done.returncode != 0 or not out.exists() or out.stat().st_size == 0:
        raise RuntimeError(done.stderr.strip()[:200] or "ffmpeg a échoué sans message")
    height, length = _probe(out)
    if length < duration_s - SHORT_BY_S:
        raise TooShort(f"{length:.1f} s lues sur {duration_s} s")
    return Made(out, height, out.stat().st_size, length)


# Les POV confirmées de chaque moment publié, et l'ancre elle-même, en temps
# de leur VOD.
_POV = """
WITH pov AS (
  SELECT s.vod_id, v.started_at,
         m.start_s + extract(epoch FROM a.started_at - v.started_at) + s.offset_ms / 1000.0
           AS start_s,
         m.end_s + extract(epoch FROM a.started_at - v.started_at) + s.offset_ms / 1000.0
           AS end_s
  FROM discovered_moments m
  JOIN discovered_events e ON e.id = m.event_id AND e.match_id IS NOT NULL
  JOIN discovered_vods a ON a.vod_id = e.anchor_vod_id
  JOIN discovered_sources s ON s.event_id = e.id AND s.status = 'confirmed'
  JOIN discovered_vods v ON v.vod_id = s.vod_id
  WHERE v.availability <> 'expired' AND s.vod_id <> e.anchor_vod_id
  UNION ALL
  SELECT a.vod_id, a.started_at, m.start_s, m.end_s
  FROM discovered_moments m
  JOIN discovered_events e ON e.id = m.event_id AND e.match_id IS NOT NULL
  JOIN discovered_vods a ON a.vod_id = e.anchor_vod_id
  WHERE a.availability <> 'expired'
)
"""

# Celles des VOD Twitch qu'aucun extrait fait ne recouvre.
_UNCOVERED = """
FROM pov p
WHERE p.vod_id ~ '^[0-9]+$'
  AND (%(vods)s::text[] IS NULL OR p.vod_id = ANY(%(vods)s::text[]))
  AND NOT EXISTS (
    SELECT 1 FROM pov_clips c
    WHERE c.vod_id = p.vod_id AND c.status = 'done'
      AND c.start_s <= p.start_s AND p.end_s <= c.start_s + c.duration_s)
"""

# Les VOD les plus anciennes d'abord : ce sont elles que Twitch efface en premier.
_PENDING = f"""{_POV}
SELECT p.vod_id, p.start_s, p.end_s {_UNCOVERED}
ORDER BY p.started_at, p.vod_id, p.start_s
LIMIT %(limit)s
"""

# Une fenêtre abandonnée reste comptée ici : sa VOD est listée, puis `missing`
# l'écarte sans la lire.
_PENDING_VODS = f"""{_POV}
SELECT p.vod_id {_UNCOVERED}
GROUP BY p.vod_id
ORDER BY min(p.started_at), p.vod_id
LIMIT %(limit)s
"""

# Les VOD d'ancre (le broadcast) : leurs extraits servent aussi d'aperçus.
_ANCHORS = """
SELECT DISTINCT anchor_vod_id FROM discovered_events
WHERE anchor_vod_id = ANY(%(vods)s::text[])
"""

_KNOWN = """
SELECT vod_id, start_s, duration_s, status, attempts FROM pov_clips
WHERE vod_id = ANY(%(vods)s::text[])
"""

_RECORD = """
INSERT INTO pov_clips (vod_id, start_s, duration_s, status, height, bytes, error)
VALUES (%(vod_id)s, %(start_s)s, %(duration_s)s, %(status)s, %(height)s, %(bytes)s, %(error)s)
ON CONFLICT (vod_id, start_s, duration_s) DO UPDATE SET
  status = EXCLUDED.status, height = EXCLUDED.height, bytes = EXCLUDED.bytes,
  error = EXCLUDED.error, updated_at = now(),
  attempts = CASE WHEN EXCLUDED.status = 'failed'
                  THEN least(pov_clips.attempts + 1, 10) ELSE pov_clips.attempts END
"""


_EXPIRE = """
UPDATE discovered_vods SET availability = 'expired', availability_checked_at = now(),
  updated_at = now()
WHERE vod_id = %(vod_id)s
"""


def is_gone(message: str) -> bool:
    """Le refus de yt-dlp pour une VOD que Twitch n'a plus (« Video N does not exist »)."""
    return "does not exist" in message


def twitch_source(vod_id: str) -> str:
    """La playlist vidéo de la VOD ; `VodGone` si Twitch l'a effacée."""
    try:
        return moment_previews.twitch_source(vod_id)
    except Exception as error:
        if is_gone(str(error)):
            raise VodGone(str(error)[:200]) from error
        raise


def pending(connection: Connection, vods: Iterable[str] | None, limit: int) -> list[Wanted]:
    """Les moments × POV sans extrait, les VOD les plus anciennes d'abord."""
    only = list(vods) if vods is not None else None
    cursor = connection.execute(_PENDING, {"vods": only, "limit": limit})
    rows = cursor.fetchall()  # type: ignore[attr-defined]
    return [Wanted(str(r[0]), float(r[1]), float(r[2])) for r in rows]


def pending_vods(connection: Connection, limit: int) -> list[str]:
    """Les VOD qui ont encore des extraits à couper, les plus anciennes d'abord."""
    cursor = connection.execute(_PENDING_VODS, {"vods": None, "limit": limit})
    return [str(row[0]) for row in cursor.fetchall()]  # type: ignore[attr-defined]


def _known(
    connection: Connection, vods: list[str]
) -> tuple[dict[str, list[Window]], dict[str, set[Window]]]:
    """Par VOD : les extraits faits, et les fenêtres qui ont trop échoué."""
    made: dict[str, list[Window]] = {}
    given_up: dict[str, set[Window]] = {}
    if not vods:
        return made, given_up
    cursor = connection.execute(_KNOWN, {"vods": vods})
    for vod, start, duration, status, attempts in cursor.fetchall():  # type: ignore[attr-defined]
        window = (int(start), int(duration))
        if status == "done":
            made.setdefault(str(vod), []).append(window)
        elif int(attempts) >= MAX_ATTEMPTS:
            given_up.setdefault(str(vod), set()).add(window)
    return made, given_up


def anchors(connection: Connection, vods: list[str]) -> set[str]:
    """Celles des VOD qui sont l'ancre (le broadcast) d'un event."""
    if not vods:
        return set()
    cursor = connection.execute(_ANCHORS, {"vods": vods})
    return {str(row[0]) for row in cursor.fetchall()}  # type: ignore[attr-defined]


def storage_class(vod_id: str, broadcast: Set[str]) -> str | None:
    """Standard pour le broadcast (lu par les aperçus), accès peu fréquent sinon.

    Un co-stream n'est relu qu'après l'expiration de sa VOD
    (docs/plans/stockage-video.md).
    """
    return None if vod_id in broadcast else INFREQUENT


def _record(
    connection: Connection,
    vod_id: str,
    window: Window,
    *,
    made: Made | None = None,
    error: str | None = None,
) -> None:
    connection.execute(
        _RECORD,
        {
            "vod_id": vod_id,
            "start_s": window[0],
            "duration_s": window[1],
            "status": "done" if made is not None else "failed",
            "height": made.height if made is not None else None,
            "bytes": made.bytes if made is not None else None,
            "error": error[:300] if error is not None else None,
        },
    )


def run(
    connection: Connection,
    storage: Uploader,
    *,
    vods: Iterable[str] | None = None,
    limit: int = CLIPS_PER_PASS,
    resolve: Callable[[str], str] = twitch_source,
    time_left: Callable[[], float] | None = None,
    preset: str = moment_previews.PRESET,
) -> Report:
    """Coupe, range et inscrit les extraits manquants ; un raté n'arrête rien."""
    report = Report()
    wanted = pending(connection, vods, limit)
    vod_ids = sorted({w.vod_id for w in wanted})
    made, given_up = _known(connection, vod_ids)
    broadcast = anchors(connection, vod_ids)
    for vod_id, windows in missing(wanted, made, given_up=given_up).items():
        if time_left is not None and time_left() < COMMAND_TIMEOUT_S:
            break
        try:
            source = resolve(vod_id)
        except VodGone as error:
            log.warning("VOD %s effacée : %s", vod_id, error)
            connection.execute(_EXPIRE, {"vod_id": vod_id})
            report.gone += 1
            continue
        except Exception as error:  # noqa: BLE001 — une VOD illisible n'arrête pas les autres
            log.warning("VOD %s illisible : %s", vod_id, error)
            for window in windows:
                _record(connection, vod_id, window, error=f"VOD illisible : {error}")
            report.failed += len(windows)
            continue
        for window in windows:
            if time_left is not None and time_left() < COMMAND_TIMEOUT_S:
                break
            try:
                with tempfile.TemporaryDirectory() as tmp:
                    clip = cut(source, window[0], window[1], Path(tmp), preset)
                    storage.upload(
                        clip.path,
                        clip_key(vod_id, *window),
                        storage_class=storage_class(vod_id, broadcast),
                    )
            except Exception as error:  # noqa: BLE001 — un extrait raté n'arrête pas les autres
                log.warning("extrait %s %s raté : %s", vod_id, window, error)
                _record(connection, vod_id, window, error=f"{type(error).__name__}: {error}")
                report.failed += 1
                continue
            _record(connection, vod_id, window, made=clip)
            report.made += 1
    if report.made:
        # Les aperçus couverts par un nouvel extrait du broadcast le liront
        # (docs/plans/stockage-video.md, item 5).
        moment_previews.link_clips(connection)
    return report


# Les extraits faits des co-streams (docs/plans/stockage-video.md, item 4) :
# ceux du broadcast restent en standard, les aperçus les lisent.
_COSTREAM_CLIPS = """
SELECT c.vod_id, c.start_s, c.duration_s, coalesce(c.bytes, 0) FROM pov_clips c
WHERE c.status = 'done' AND c.vod_id ~ '^[0-9]+$'
  AND NOT EXISTS (SELECT 1 FROM discovered_events e WHERE e.anchor_vod_id = c.vod_id)
ORDER BY c.vod_id, c.start_s
"""


class Reclassing(Protocol):
    def storage_class_of(self, key: str) -> str: ...
    def set_storage_class(self, key: str, storage_class: str) -> None: ...


@dataclass
class Reclassed:
    moved: int = 0
    bytes: int = 0
    already: int = 0
    failed: int = 0


def to_infrequent(
    connection: Connection,
    storage: Reclassing,
    *,
    apply: bool,
    time_left: Callable[[], float] | None = None,
) -> Reclassed:
    """Passe en accès peu fréquent les extraits des co-streams encore en standard.

    À blanc (`apply=False`), compte seulement. Un extrait déjà passé n'est pas
    recopié : la copie relirait ses octets, et cette relecture se paie. On peut
    donc relancer après une coupure.
    """
    report = Reclassed()
    cursor = connection.execute(_COSTREAM_CLIPS)
    for vod_id, start, duration, size in cursor.fetchall():  # type: ignore[attr-defined]
        if time_left is not None and time_left() <= 0:
            break
        key = clip_key(str(vod_id), int(start), int(duration))
        try:
            if storage.storage_class_of(key) == INFREQUENT:
                report.already += 1
                continue
            if apply:
                storage.set_storage_class(key, INFREQUENT)
        except Exception as error:  # noqa: BLE001 — un extrait raté n'arrête pas les autres
            log.warning("extrait %s : classe inchangée (%s)", key, error)
            report.failed += 1
            continue
        report.moved += 1
        report.bytes += int(size)
    return report
