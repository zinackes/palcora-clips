"""Les aperçus des moments publiés (docs/plans/page-moments.md, item 1).

Pour chaque moment de `discovered_moments` : une image JPEG (1280 px) et le
moment entier en vidéo muette (1080p au plus, borné par `PREVIEW_MAX_S`),
tirés de la VOD officielle (l'ancre de l'event), puis posés dans R2 sous
`previews/<id>.jpg|mp4`. Leur présence se note dans `signals.preview`
(`{"version": 2, "duration_s": …}`) ; un moment republié change d'id et
reprend donc sans aperçu, ce qui est voulu.

Quand l'extrait du broadcast (`pov_clips`) couvre tout l'aperçu,
`signals.preview.clip` note sa fenêtre et le site le lit à la place du
`.mp4` (docs/plans/stockage-video.md) : la même vidéo n'est plus rangée deux
fois. `link_clips` le note après coup, un aperçu fait après l'extrait n'a
plus que son image.

Chaque image a aussi sa vignette de 480 px, `previews/<id>.sm.jpg`, pour les
grilles du site qui l'affichent en ~300 px (plan alleger-pages, item 5) :
l'image de 1280 px pesait ~120 ko par carte. `signals.preview.thumb` la note ;
`thumbs` fait celles des aperçus d'avant.

Version 1 (jusqu'au 25/11/2026) : 4 s en 640 px, jugées floues et trop
courtes par Mathys. `--redo` les refait tant que la VOD vit ; un raté garde
l'ancien extrait (docs/plans/landing-retouches.md, item 6).

ffmpeg lit directement la playlist HLS : avec `-ss` avant `-i`, il ne tire
que les segments utiles (≈ 1,5 s l'image, 2 s pour 4 s de clip, mesuré le
19/11/2026).
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from psycopg.types.json import Jsonb

from worker.ingest.download import NoPlaylist, checked_playlist
from worker.limits import PREVIEW_MAX_S

log = logging.getLogger(__name__)

FRAME_WIDTH = 1280
# La vignette des grilles : ~300 px affichés, 480 couvre un écran 1,5×.
THUMB_WIDTH = 480
MAX_HEIGHT = 1080
# Lire jusqu'à 2 min de HLS et les encoder : large marge sur 2 cœurs.
COMMAND_TIMEOUT_S = 300
VERSION = 2
# Les réglages vidéo des aperçus et des extraits (docs/plans/stockage-video.md) :
# même CRF et même plafond qu'avant, preset `slow` et une image clé toutes les
# 2 s (au lieu de chaque seconde) : -10 à -48 % sous le plafond, même qualité
# mesurée le 26/11/2026. H.264 reste : HEVC ne se lit pas partout.
PRESET = "slow"


def video_args(preset: str = PRESET) -> tuple[str, ...]:
    """Les réglages vidéo ; le PC coupe en `faster` (banc du 26/11/2026 :
    2,9 fois plus vite que `slow`, VMAF 94,5 contre 95,4, taille voisine)."""
    return (
        "-c:v", "libx264", "-crf", "23", "-preset", preset,
        "-maxrate", "5000k", "-bufsize", "10000k", "-g", "60",
        "-pix_fmt", "yuv420p",
    )  # fmt: skip


VIDEO_ARGS = video_args()


class NoVideo(RuntimeError):
    """La VOD n'a pas de variante vidéo lisible ici."""


class Uploader(Protocol):
    def upload(self, path: Path, key: str) -> None: ...


def preview_times(start_s: float, end_s: float) -> tuple[float, float, float]:
    """L'instant de l'image, le début du clip et sa durée.

    L'image à 45 % : les moments s'ouvrent sur la mise en place et finissent
    sur la réaction ; le cœur de l'action est un peu avant le milieu. Le clip
    est le moment entier, de son début, borné par `PREVIEW_MAX_S`.
    """
    length = max(0.0, end_s - start_s)
    frame = start_s + 0.45 * length
    return frame, start_s, min(length, PREVIEW_MAX_S)


def preview_keys(moment_id: str) -> tuple[str, str]:
    return f"previews/{moment_id}.jpg", f"previews/{moment_id}.mp4"


def thumb_key(moment_id: str) -> str:
    return f"previews/{moment_id}.sm.jpg"


def video_playlist(info: Mapping[str, object]) -> str:
    """La variante vidéo la plus haute jusqu'à 1080p, servie par Twitch."""
    formats = info.get("formats")
    if not isinstance(formats, list):
        raise NoVideo("aucun format dans le probe")
    videos = [
        f
        for f in formats
        if isinstance(f, dict)
        and isinstance(f.get("height"), int)
        and 0 < f["height"] <= MAX_HEIGHT
        and isinstance(f.get("url"), str)
    ]
    if not videos:
        raise NoVideo("aucune variante vidéo jusqu'à 1080p")
    best = max(videos, key=lambda f: f["height"])
    try:
        return checked_playlist(best["url"])
    except NoPlaylist as error:
        raise NoVideo(str(error)) from error


# La playlist vient d'un tiers : ses segments ne lisent que du HTTPS, jamais
# un fichier local ni un autre protocole (audit 2026-11-19, lot 6). Un chemin
# local (tests, fichier déjà téléchargé) vient de nous et garde « file ».
def _input_guard(source: str) -> tuple[str, ...]:
    if source.startswith("https://"):
        return ("-protocol_whitelist", "https,tls,tcp,crypto")
    return ()


def frame_command(source: str, at_s: float, out: Path) -> list[str]:
    return [
        "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
        *_input_guard(source), "-ss", f"{at_s:.3f}", "-i", source,
        "-frames:v", "1", "-vf", f"scale={FRAME_WIDTH}:-2", "-q:v", "5",
        str(out),
    ]  # fmt: skip


def clip_command(source: str, at_s: float, length_s: float, out: Path) -> list[str]:
    # 1080p au plus (Mathys, 25/11), la hauteur de la source sinon ; débit
    # plafonné pour qu'un moment de 40 s pèse ~25 Mo au plus (la redirection R2
    # sert les requêtes partielles).
    return [
        "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
        *_input_guard(source), "-ss", f"{at_s:.3f}", "-i", source, "-t", f"{length_s:.3f}",
        "-an", "-vf", f"scale=-2:min(ih\\,{MAX_HEIGHT}),fps=30",
        *VIDEO_ARGS, "-movflags", "+faststart",
        str(out),
    ]  # fmt: skip


def thumb_command(image: Path, out: Path) -> list[str]:
    return [
        "ffmpeg", "-nostdin", "-y", "-loglevel", "error", "-i", str(image),
        "-vf", f"scale={THUMB_WIDTH}:-2", "-q:v", "5", str(out),
    ]  # fmt: skip


def make_thumb(image: Path, out: Path) -> Path:
    """La vignette des grilles, tirée de l'image de l'aperçu."""
    _run(thumb_command(image, out))
    return out


def _run(command: list[str]) -> None:
    done = subprocess.run(  # noqa: S603 — liste d'arguments, jamais de shell
        command, capture_output=True, text=True, timeout=COMMAND_TIMEOUT_S, check=False
    )
    out = Path(command[-1])
    if done.returncode != 0 or not out.exists() or out.stat().st_size == 0:
        raise RuntimeError(done.stderr.strip()[:200] or "ffmpeg a échoué sans message")


def grab(source: str, start_s: float, end_s: float, workdir: Path) -> tuple[Path, Path, float]:
    """L'image et le clip d'un moment, dans `workdir`, et la durée du clip."""
    frame_at, clip_at, length = preview_times(start_s, end_s)
    image = workdir / "preview.jpg"
    clip = workdir / "preview.mp4"
    _run(frame_command(source, frame_at, image))
    _run(clip_command(source, clip_at, length, clip))
    return image, clip, length


@dataclass(frozen=True)
class Pending:
    moment_id: str
    vod_id: str
    start_s: float
    end_s: float
    # L'extrait du broadcast qui couvre tout l'aperçu, s'il est déjà fait
    # (`{"vod", "start_s", "duration_s"}`) : l'aperçu le lira.
    clip: Mapping[str, object] | None = None


# L'extrait du broadcast qui couvre l'aperçu du moment `m` : son début et
# `length` secondes (docs/plans/stockage-video.md, item 5). Le plus court
# s'il y en a plusieurs, il se charge plus vite.
def _covering_clip(length: str) -> str:
    return f"""(
  SELECT jsonb_build_object('vod', c.vod_id, 'start_s', c.start_s, 'duration_s', c.duration_s)
  FROM pov_clips c
  WHERE c.vod_id = e.anchor_vod_id AND c.status = 'done'
    AND c.start_s <= m.start_s AND m.start_s + {length} <= c.start_s + c.duration_s
  ORDER BY c.duration_s, c.start_s LIMIT 1)"""


_PENDING = f"""
SELECT m.id::text, e.anchor_vod_id, m.start_s, m.end_s,
  {_covering_clip(f"least(m.end_s - m.start_s, {PREVIEW_MAX_S})")}
FROM discovered_moments m JOIN discovered_events e ON e.id = m.event_id
WHERE (NOT (m.signals ? 'preview')
       OR (%(redo)s AND COALESCE((m.signals->'preview'->>'version')::int, 1) < %(version)s))
  AND (%(events)s::uuid[] IS NULL OR e.id = ANY(%(events)s::uuid[]))
  AND e.match_id IS NOT NULL
-- Les moments sans aperçu d'abord, puis les plus récents : leur VOD vit encore.
ORDER BY m.signals ? 'preview', e.anchor_vod_id DESC, m.start_s
LIMIT %(limit)s
"""

_MARK = """
UPDATE discovered_moments SET signals = signals || %(preview)s WHERE id = %(id)s
"""

# Les aperçus (version 2 : le moment entier, de son début) qu'un extrait du
# broadcast couvre désormais : le site lira l'extrait, le `.mp4` devient inutile.
_LINK = f"""
WITH found AS (
  SELECT m.id, {_covering_clip("(m.signals->'preview'->>'duration_s')::float")} clip
  FROM discovered_moments m JOIN discovered_events e ON e.id = m.event_id
  WHERE m.signals ? 'preview' AND NOT (m.signals->'preview' ? 'clip')
    AND COALESCE((m.signals->'preview'->>'version')::int, 1) >= 2
    AND m.signals->'preview' ? 'duration_s'
)
UPDATE discovered_moments m SET signals = jsonb_set(m.signals, '{{preview,clip}}', f.clip)
FROM found f WHERE m.id = f.id AND f.clip IS NOT NULL
"""


class Connection(Protocol):
    def execute(self, query: str, params: Mapping[str, object] | None = None) -> object: ...


def pending(
    connection: Connection, events: Iterable[str] | None, limit: int, *, redo: bool = False
) -> list[Pending]:
    """Les moments sans aperçu ; avec `redo`, aussi ceux d'une version passée."""
    wanted = list(events) if events is not None else None
    cursor = connection.execute(
        _PENDING, {"events": wanted, "limit": limit, "redo": redo, "version": VERSION}
    )
    rows = cursor.fetchall()  # type: ignore[attr-defined]
    return [
        Pending(str(r[0]), str(r[1]), float(r[2]), float(r[3]), r[4] if len(r) > 4 else None)
        for r in rows
    ]


def link_clips(connection: Connection) -> int:
    """Note sur chaque aperçu l'extrait du broadcast qui le couvre ; rend leur nombre."""
    cursor = connection.execute(_LINK)
    return max(0, int(getattr(cursor, "rowcount", 0) or 0))


def twitch_source(vod_id: str) -> str:
    """La playlist vidéo d'une VOD Twitch, résolue par yt-dlp (jetons compris)."""
    import yt_dlp

    opts = {"quiet": True, "no_warnings": True, "skip_download": True}
    with yt_dlp.YoutubeDL(opts) as probe:
        info = probe.extract_info(f"https://www.twitch.tv/videos/{vod_id}", download=False) or {}
    return video_playlist(info)


def run(
    connection: Connection,
    storage: Uploader,
    *,
    events: Iterable[str] | None = None,
    limit: int = 500,
    redo: bool = False,
    resolve: Callable[[str], str] = twitch_source,
) -> tuple[int, int]:
    """Fait les aperçus manquants (et, avec `redo`, refait les anciens).

    Rend (faits, ratés) ; un raté n'arrête rien et garde l'aperçu d'avant.
    """
    done = failed = 0
    sources: dict[str, str | None] = {}
    for item in pending(connection, events, limit, redo=redo):
        if item.vod_id not in sources:
            try:
                sources[item.vod_id] = resolve(item.vod_id)
            except Exception as error:  # noqa: BLE001 — une VOD expirée n'arrête pas les autres
                log.warning("VOD %s illisible : %s", item.vod_id, error)
                sources[item.vod_id] = None
        source = sources[item.vod_id]
        if source is None:
            failed += 1
            continue
        try:
            with tempfile.TemporaryDirectory() as tmp:
                image_key, clip_key = preview_keys(item.moment_id)
                if item.clip is not None:
                    # L'extrait du broadcast sert d'aperçu : l'image seule.
                    frame_at, _, length = preview_times(item.start_s, item.end_s)
                    image = Path(tmp) / "preview.jpg"
                    _run(frame_command(source, frame_at, image))
                    storage.upload(image, image_key)
                    storage.upload(
                        make_thumb(image, Path(tmp) / "thumb.jpg"), thumb_key(item.moment_id)
                    )
                else:
                    image, clip, length = grab(source, item.start_s, item.end_s, Path(tmp))
                    storage.upload(image, image_key)
                    storage.upload(
                        make_thumb(image, Path(tmp) / "thumb.jpg"), thumb_key(item.moment_id)
                    )
                    storage.upload(clip, clip_key)
        except Exception as error:  # noqa: BLE001 — un moment raté n'arrête pas les autres
            log.warning("aperçu %s raté : %s", item.moment_id, error)
            failed += 1
            continue
        mark: dict[str, object] = {
            "version": VERSION,
            "duration_s": round(length, 3),
            "thumb": True,
        }
        if item.clip is not None:
            mark["clip"] = dict(item.clip)
        connection.execute(_MARK, {"id": item.moment_id, "preview": Jsonb({"preview": mark})})
        done += 1
    return done, failed


# Les aperçus faits avant les vignettes (plan alleger-pages, item 5).
_THUMBS_PENDING = """
SELECT m.id::text FROM discovered_moments m
WHERE m.signals ? 'preview' AND NOT COALESCE((m.signals->'preview'->>'thumb')::boolean, false)
ORDER BY m.id
LIMIT %(limit)s
"""

_MARK_THUMB = """
UPDATE discovered_moments SET signals = jsonb_set(signals, '{preview,thumb}', 'true')
WHERE id = %(id)s AND signals ? 'preview'
"""


class Store(Uploader, Protocol):
    def download(self, key: str, path: Path) -> None: ...


def thumbs(connection: Connection, storage: Store, *, limit: int = 500) -> tuple[int, int]:
    """Fait la vignette des aperçus qui n'en ont pas, depuis leur image dans R2.

    Sans VOD : l'image de 1280 px suffit, donc une VOD expirée n'empêche
    rien. Rend (faites, ratées) ; une ratée n'arrête pas les autres.
    """
    cursor = connection.execute(_THUMBS_PENDING, {"limit": limit})
    done = failed = 0
    for (moment_id,) in cursor.fetchall():  # type: ignore[attr-defined]
        try:
            with tempfile.TemporaryDirectory() as tmp:
                image = Path(tmp) / "preview.jpg"
                storage.download(preview_keys(moment_id)[0], image)
                storage.upload(make_thumb(image, Path(tmp) / "thumb.jpg"), thumb_key(moment_id))
        except Exception as error:  # noqa: BLE001 — un aperçu raté n'arrête pas les autres
            log.warning("vignette %s ratée : %s", moment_id, error)
            failed += 1
            continue
        connection.execute(_MARK_THUMB, {"id": moment_id})
        done += 1
    return done, failed


def r2_endpoint(env: Mapping[str, str]) -> str:
    """L'adresse R2 : `R2_ENDPOINT_URL` (Modal), sinon dérivée de
    `R2_ACCOUNT_ID` comme le fait cloud.sh. Le .env du PC n'a que la seconde
    (`.env.example`) : la commande s'y lance telle quelle."""
    if env.get("R2_ENDPOINT_URL"):
        return env["R2_ENDPOINT_URL"]
    account = env.get("R2_ACCOUNT_ID", "")
    if not re.fullmatch(r"[0-9a-f]{32}", account):
        raise KeyError("R2_ENDPOINT_URL ou R2_ACCOUNT_ID (32 caractères hexadécimaux) requis")
    return f"https://{account}.r2.cloudflarestorage.com"


def storage_from_env() -> Store:
    from worker.storage import R2Storage

    return R2Storage.from_credentials(
        bucket=os.environ["R2_BUCKET"],
        endpoint_url=r2_endpoint(os.environ),
        access_key_id=os.environ["R2_ACCESS_KEY_ID"],
        secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    )


# Les teasers de l'affiche de la landing (audit du 27/11, P1-12) : 6 s en
# 720p autour du cœur de l'action, ~1 Mo, au lieu du moment entier en 1080p
# (20,5 Mo téléchargés par visite sur ordinateur). `previews/<id>.teaser.mp4`,
# noté `signals.preview.teaser` (sa durée). Tiré de la vidéo déjà dans R2 :
# l'aperçu, ou l'extrait du broadcast qui le couvre ; aucune VOD.
TEASER_S = 6
TEASER_HEIGHT = 720
TEASER_ARGS = (
    "-c:v", "libx264", "-crf", "28", "-preset", "slow",
    "-maxrate", "1500k", "-bufsize", "3000k", "-g", "60", "-pix_fmt", "yuv420p",
)  # fmt: skip

_TEASERS_PENDING = """
SELECT m.id::text, m.start_s, m.end_s, m.signals->'preview'->'clip',
  (m.signals->'preview'->>'duration_s')::float
FROM discovered_moments m
WHERE m.signals ? 'preview' AND NOT (m.signals->'preview' ? 'teaser')
  AND COALESCE((m.signals->'preview'->>'version')::int, 1) >= 2
  AND (%(events)s::uuid[] IS NULL OR m.event_id = ANY(%(events)s::uuid[]))
  -- Une part du rattrapage : plusieurs conteneurs, aucun moment en double.
  AND mod(abs(hashtext(m.id::text)), %(shards)s) = %(shard)s
ORDER BY m.created_at DESC, m.id
LIMIT %(limit)s
"""

_MARK_TEASER = """
UPDATE discovered_moments SET signals = jsonb_set(signals, '{preview,teaser}', %(s)s)
WHERE id = %(id)s AND signals ? 'preview'
"""


def teaser_key(moment_id: str) -> str:
    return f"previews/{moment_id}.teaser.mp4"


def teaser_source(
    moment_id: str, start_s: float, end_s: float, clip: object, length_s: float | None
) -> tuple[str, float]:
    """La clé R2 de la vidéo à lire et l'instant (dans cette vidéo) où le
    teaser commence : 3 s avant l'image de l'aperçu (45 % du moment)."""
    length = min(max(0.0, end_s - start_s), length_s or PREVIEW_MAX_S)
    within = max(0.0, min(0.45 * length - TEASER_S / 2, length - TEASER_S))
    if isinstance(clip, Mapping) and {"vod", "start_s", "duration_s"} <= clip.keys():
        from worker.pov_clips import clip_key

        key = clip_key(str(clip["vod"]), int(clip["start_s"]), int(clip["duration_s"]))
        return key, start_s - float(clip["start_s"]) + within
    return preview_keys(moment_id)[1], within


def teaser_command(source: str, at_s: float, out: Path) -> list[str]:
    return [
        "ffmpeg", "-nostdin", "-y", "-loglevel", "error",
        *_input_guard(source), "-ss", f"{at_s:.3f}", "-i", source, "-t", str(TEASER_S),
        "-an", "-vf", f"scale=-2:min(ih\\,{TEASER_HEIGHT}),fps=30",
        *TEASER_ARGS, "-movflags", "+faststart", str(out),
    ]  # fmt: skip


class SignedStore(Store, Protocol):
    def signed_url(self, key: str, expires_s: int = 900) -> str: ...


def teasers(
    connection: Connection,
    storage: SignedStore,
    *,
    events: Iterable[str] | None = None,
    limit: int = 500,
    shard: int = 0,
    shards: int = 1,
) -> tuple[int, int]:
    """Fait les teasers qui manquent (la part `shard` sur `shards`) ; rend
    (faits, ratés)."""
    wanted = list(events) if events is not None else None
    cursor = connection.execute(
        _TEASERS_PENDING, {"events": wanted, "limit": limit, "shard": shard, "shards": shards}
    )
    done = failed = 0
    for moment_id, start_s, end_s, clip, length in cursor.fetchall():  # type: ignore[attr-defined]
        key, at = teaser_source(moment_id, float(start_s), float(end_s), clip, length)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp) / "teaser.mp4"
                _run(teaser_command(storage.signed_url(key), at, out))
                storage.upload(out, teaser_key(moment_id))
        except Exception as error:  # noqa: BLE001 — un teaser raté n'arrête pas les autres
            log.warning("teaser %s raté : %s", moment_id, error)
            failed += 1
            continue
        connection.execute(_MARK_TEASER, {"id": moment_id, "s": Jsonb(TEASER_S)})
        done += 1
    return done, failed
