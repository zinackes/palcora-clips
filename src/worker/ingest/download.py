"""Téléchargement de la seule fenêtre qui nous intéresse, pas de la VOD entière.

Une VOD d'esport fait des heures ; on en veut deux minutes. `download_ranges`
fait seeker ffmpeg directement au bon fragment — mesuré sur un runner GitHub :
14 s pour trois minutes de 720p60 tirées d'une VOD de 9 h 34
(docs/runbooks/real-dataset.md §5).

Attention au nom : `--download-sections` est l'option **CLI**. En bibliothèque
c'est `download_ranges`, qui attend un *callback*, pas la chaîne de la CLI.

Les bornes rendues par yt-dlp sont alignées sur les keyframes, donc imprécises
de quelques secondes. D'où la marge, et surtout : on ne se fie jamais au
`start_s` demandé pour dater le contenu obtenu — on coupe précisément plus tard,
après alignement. On ne passe volontairement pas `force_keyframes_at_cuts`, qui
réencode et coûte cher.
"""

from __future__ import annotations

import math
import os
import subprocess
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from worker.ingest import hls
from worker.ingest.twitch import parse_vod_url

DEFAULT_CACHE_DIR = Path("media/cache")
DEFAULT_TIMEOUT_S = 300.0
DEFAULT_MARGIN_S = 15
# Ce qu'une fenêtre laisse entre sa fin et le direct en cours. La durée lue au
# probe vieillit pendant que ffmpeg ouvre la playlist, et les derniers segments
# ne sont pas toujours servis tout de suite.
LIVE_EDGE_MARGIN_S = 180

_MAX_HEIGHT = 720
_CONCURRENT_FRAGMENTS = 4


class LiveStreamRefused(RuntimeError):
    """Le stream est encore en direct : sa playlist n'est pas figée, rien à extraire."""


class NoPlaylist(RuntimeError):
    """Le probe n'a rendu aucune playlist média : extracteur cassé ou VOD fermée."""


class CutFailed(RuntimeError):
    """ffmpeg n'a pas su recouper les segments aux bornes de la fenêtre."""


class DownloadTimeout(TimeoutError):
    """Le téléchargement a dépassé son budget de temps et a été interrompu."""


@dataclass(frozen=True)
class Window:
    """La fenêtre effectivement demandée à yt-dlp, marge comprise."""

    vod_id: str
    start_s: int
    end_s: int

    @property
    def filename(self) -> str:
        return f"{self.vod_id}_{self.start_s}-{self.end_s}.mp4"


def plan_window(url: str, start_s: int, dur_s: int, margin_s: int = DEFAULT_MARGIN_S) -> Window:
    """Calcule les bornes à demander. Pur, donc testable sans réseau."""
    vod_id = parse_vod_url(url)
    start = max(0, int(start_s) - int(margin_s))
    end = int(start_s) + int(dur_s) + int(margin_s)
    if end <= start:
        raise ValueError(f"fenêtre vide : {start} → {end}")
    return Window(vod_id=vod_id, start_s=start, end_s=end)


def live_window(info: Mapping[str, object], url: str, window: Window) -> bool:
    """Dit si la fenêtre doit être tirée en mode « direct », et refuse sinon.

    Leçon de ZIN-28 : ffmpeg démarre trois segments avant la fin d'une playlist
    non close (`live_start_index` vaut -3), s'y colle et consomme un fragment de
    10 s toutes les 10 s, indéfiniment. Ce n'est pas la playlist qui manque de
    contenu : relevée le 2026-09-20 sur un direct, elle porte tous ses segments
    depuis la première seconde. Lui dire de partir du début (`live_from_start`)
    rend la fenêtre extractible, à deux conditions :

    - une durée connue, sinon yt-dlp ne passe pas de `-t` et le téléchargement
      court jusqu'au live edge, puis suit le direct ;
    - une fin de fenêtre assez en arrière de ce live edge, parce que la durée
      lue au probe a déjà vieilli quand ffmpeg ouvre la playlist.
    """
    if not info.get("is_live"):
        return False
    duration = info.get("duration")
    if not isinstance(duration, int | float) or not math.isfinite(duration) or duration <= 0:
        raise LiveStreamRefused(f"{url} est en direct et sa durée est inconnue — après la fin")
    if window.end_s > duration - LIVE_EDGE_MARGIN_S:
        raise LiveStreamRefused(
            f"{url} : la fenêtre {window.start_s}-{window.end_s} s touche le direct en cours "
            f"({int(duration)} s enregistrées) — au prochain passage"
        )
    return True


def deadline_guard(deadline: float) -> Callable[[Mapping[str, object]], None]:
    """Hook de progression qui interrompt un téléchargement trop long.

    yt-dlp n'a pas de plafond en temps mural : `socket_timeout` ne couvre qu'une
    connexion muette, pas un flux qui arrive sagement mais sans fin. Le hook est
    appelé à chaque fragment, ce qui en fait le seul vrai arrêt dur disponible.
    """

    def guard(_status: Mapping[str, object]) -> None:
        if time.monotonic() > deadline:
            raise DownloadTimeout("téléchargement au-delà de son budget de temps, interrompu")

    return guard


def download_options(
    window: Window, out: Path, *, audio_only: bool, timeout_s: float, live: bool = False
) -> dict[str, object]:
    """Les options yt-dlp d'une fenêtre. L'audio seul prend la piste audio de
    Twitch, et à défaut la plus petite vidéo : jamais du 720p pour une écoute."""
    from yt_dlp.utils import download_range_func

    opts: dict[str, object] = {
        "download_ranges": download_range_func([], [(window.start_s, window.end_s)]),
        "concurrent_fragment_downloads": _CONCURRENT_FRAGMENTS,
        "noplaylist": True,
        "outtmpl": str(out),
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [deadline_guard(time.monotonic() + timeout_s)],
    }
    if live:
        # `-live_start_index 0` : partir du premier segment de la playlist, pas
        # du live edge. Sans cette option la fenêtre demandée est ignorée.
        opts["live_from_start"] = True
    if audio_only:
        opts["format"] = "ba/worst"
    else:
        opts["format"] = f"bv*[height<={_MAX_HEIGHT}]+ba/b[height<={_MAX_HEIGHT}]"
        opts["merge_output_format"] = "mp4"
    return opts


def playlist_url(info: Mapping[str, object]) -> str:
    """La playlist média à lire : la variante audio de Twitch si elle est là.

    La confirmation n'écoute que le son ; tirer des segments vidéo coûterait
    vingt fois le poids pour le même verdict.
    """
    formats = info.get("formats")
    if not isinstance(formats, list) or not formats:
        raise NoPlaylist("aucun format dans le probe")
    audio = next(
        (f for f in formats if isinstance(f, dict) and f.get("format_id") == "Audio_Only"),
        None,
    )
    chosen = audio if audio is not None else formats[0]
    url = chosen.get("url") if isinstance(chosen, dict) else None
    if not isinstance(url, str) or not url:
        raise NoPlaylist("le format retenu n'a pas d'URL")
    return checked_playlist(url)


def checked_playlist(url: str) -> str:
    """Une URL de playlist servie par Twitch ou son CloudFront, en HTTPS ; sinon refus."""
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or ""
        # yt-dlp Twitch VODs use CloudFront distribution hosts. Subsequent
        # requests are pinned to this exact host in ingest.hls.
        twitch_host = host in ("twitch.tv", "ttvnw.net") or host.endswith(
            (".twitch.tv", ".ttvnw.net")
        )
        cloudfront_host = host.endswith(".cloudfront.net") and host.count(".") == 2
        allowed = (
            parsed.scheme == "https"
            and (twitch_host or cloudfront_host)
            and parsed.username is None
            and parsed.password is None
            and parsed.port in (None, 443)
        )
    except ValueError:
        allowed = False
    if not allowed:
        raise NoPlaylist("hôte ou protocole de playlist HLS non autorisé")
    return url


def cut_command(raw: Path, out: Path, *, offset_s: float, duration_s: float) -> list[str]:
    """Recoupe les segments aux bornes de la fenêtre. Pure, donc testable sans ffmpeg.

    `-c copy` garde l'encodage d'origine : la coupe tombe sur la frame audio la
    plus proche, à une vingtaine de millisecondes. La confirmation compare deux
    fenêtres au score, pas à la frame ; l'alignement fin, lui, retravaille sur
    les VOD entières.

    `-ss` et `-t` vont après `-i` : avant, ffmpeg 7.1 (l'image du worker) garde
    sur les segments Twitch des horodatages qui partent du décalage et arrête
    la copie trop tôt, d'autant (mesuré le 15/11/2026). Le fichier brut est
    local et ne couvre que la fenêtre : le lire depuis le début ne coûte rien.
    """
    return [
        "ffmpeg",
        "-nostdin",
        "-y",
        "-loglevel",
        "error",
        "-i",
        str(raw),
        "-ss",
        str(offset_s),
        "-t",
        str(duration_s),
        "-c",
        "copy",
        str(out),
    ]


def _hls_fetch(
    url: str,
    window: Window,
    out: Path,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> None:
    """Tire la fenêtre segment par segment, sans confier la découpe à ffmpeg.

    yt-dlp sert encore à résoudre la playlist — c'est lui qui connaît les
    jetons de Twitch — mais plus à télécharger : voir `ingest.hls`.
    """
    import yt_dlp

    deadline = time.monotonic() + timeout_s
    probe_opts = {"quiet": True, "no_warnings": True, "skip_download": True}
    with yt_dlp.YoutubeDL(probe_opts) as probe:
        info = probe.extract_info(url, download=False) or {}
    live_window(info, url, window)

    media = playlist_url(info)
    with tempfile.NamedTemporaryFile(
        dir=out.parent, prefix=f".{out.stem}.", suffix=".segments.mp4", delete=False
    ) as handle:
        raw = Path(handle.name)
    cut: Path | None = None
    try:
        with httpx.Client(follow_redirects=False) as client:
            playlist = hls.fetch_playlist(client, media, deadline=deadline)
            first = hls.fetch_window(
                client, media, playlist, window.start_s, window.end_s, raw, deadline=deadline
            )
        with tempfile.NamedTemporaryFile(
            dir=out.parent, prefix=f".{out.stem}.", suffix=".mp4", delete=False
        ) as handle:
            cut = Path(handle.name)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise hls.WindowTimeout("fenêtre au-delà de son budget")
        try:
            done = subprocess.run(  # noqa: S603 — liste d'arguments, jamais de shell
                cut_command(
                    raw,
                    cut,
                    offset_s=round(window.start_s - first, 3),
                    duration_s=float(window.end_s - window.start_s),
                ),
                capture_output=True,
                text=True,
                timeout=remaining,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise DownloadTimeout("coupe HLS au-delà de son budget") from exc
        if done.returncode != 0 or not cut.exists() or cut.stat().st_size == 0:
            raise CutFailed(done.stderr.strip()[:200] or "ffmpeg a échoué sans message")
        os.replace(cut, out)
    finally:
        raw.unlink(missing_ok=True)
        if cut is not None:
            cut.unlink(missing_ok=True)


def _ytdlp_fetch(
    url: str,
    window: Window,
    out: Path,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    *,
    audio_only: bool = False,
) -> None:
    """Tire la fenêtre avec yt-dlp, en réglant le cas du direct avant."""
    import yt_dlp

    probe_opts = {"quiet": True, "no_warnings": True, "skip_download": True}
    with yt_dlp.YoutubeDL(probe_opts) as probe:
        info = probe.extract_info(url, download=False)
    live = live_window(info or {}, url, window)

    opts = download_options(window, out, audio_only=audio_only, timeout_s=timeout_s, live=live)
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])


Fetch = Callable[[str, Window, Path], None]


def download_window(
    url: str,
    start_s: int,
    dur_s: int,
    *,
    margin_s: int = DEFAULT_MARGIN_S,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    force: bool = False,
    fetch: Fetch | None = None,
    audio_only: bool = False,
) -> Path:
    """Rend le chemin d'un mp4 couvrant `start_s ± margin_s`, en le tirant au besoin.

    Le cache porte sur les bornes effectives : refaire deux fois la même fenêtre
    coûterait des minutes de runner pour un fichier identique. `audio_only` tire
    la seule piste audio (la confirmation n'écoute que ça), sous son propre nom.
    """
    window = plan_window(url, start_s, dur_s, margin_s)
    name = window.filename
    out = cache_dir / (name.replace(".mp4", ".audio.mp4") if audio_only else name)

    # Un fichier vide est la trace d'un téléchargement interrompu, pas un cache.
    if out.exists() and out.stat().st_size > 0 and not force:
        return out

    cache_dir.mkdir(parents=True, exist_ok=True)
    # L'écoute passe par les segments : elle vise des chaînes qui diffusent en
    # boucle ou sont encore à l'antenne, les deux cas où `download_ranges` ne
    # rend plus de fenêtre. Le rendu garde yt-dlp, dont la découpe vidéo est
    # éprouvée sur les VOD de broadcast.
    default = _hls_fetch if audio_only else partial(_ytdlp_fetch, audio_only=False)
    (fetch or default)(url, window, out)
    return out
