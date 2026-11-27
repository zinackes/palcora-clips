"""Coupe sur le PC les extraits gardés des moments, au lieu de Modal.

Étude « Réduire la facture Modal » (26/11/2026) : un extrait coûte ~0,0037 $
sur Modal (encodage x264 + trafic sortant vers R2), rien sur le PC. Même code
que la fonction Modal `pov_clips` (`worker.pov_clips.run`), une VOD par
tâche, plusieurs VOD en parallèle, priorité basse :

    uv run --no-sync python scripts/pc-agent/clips.py --clips 20   # essai
    uv run --no-sync python scripts/pc-agent/clips.py --hours 9     # la nuit

Lit les clés comme `agent.py` (`.env.agent`, sinon `.env`). Le rôle Neon de
`.env.agent` n'écrit que les files YouTube : ce script a besoin d'un rôle qui
écrit `pov_clips` (le `DATABASE_URL` du `.env`). Ne pas le lancer pendant que
Modal coupe les mêmes VOD (`apps/worker/save_clips.py`).
"""

from __future__ import annotations

import argparse
import importlib.util
import math
import os
import platform
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType

HERE = Path(__file__).resolve().parent
# Un encodage x264 `slow` occupe environ quatre fils : au-delà, les VOD se
# gênent sans aller plus vite.
THREADS_PER_CLIP = 4
CLIPS_PER_VOD = 2000
# Neon coupe parfois une connexion pendant un long encodage (nuit du
# 26/11/2026 : `server closed the connection unexpectedly`, tout le lot tombé).
# La VOD reprend sur une connexion neuve ; ce qui est fait reste fait.
VOD_TRIES = 4
RETRY_WAIT_S = 30.0
# Sondes TCP : la connexion reste vivante pendant qu'ffmpeg encode.
KEEPALIVE = {
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}


def with_retries[T](
    attempt: Callable[[], T],
    *,
    transient: Callable[[BaseException], bool],
    tries: int = VOD_TRIES,
    wait: Callable[[float], None] = time.sleep,
    delay_s: float = RETRY_WAIT_S,
) -> T:
    """Rejoue `attempt` après une erreur passagère ; une autre erreur remonte."""
    for left in range(tries - 1, -1, -1):
        try:
            return attempt()
        except Exception as error:
            if left == 0 or not transient(error):
                raise
            print(f"coupure, nouvel essai dans {delay_s:.0f} s : {error}", flush=True)
            wait(delay_s)
    raise AssertionError("inaccessible")


def _agent() -> ModuleType:
    spec = importlib.util.spec_from_file_location("pc_agent", HERE / "agent.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def workers_for(cores: int | None) -> int:
    """Combien de VOD à la fois pour ce nombre de fils logiques."""
    return max(1, (cores or 1) // THREADS_PER_CLIP)


def per_vod_limit(clips: int | None, workers: int) -> int:
    """Le plafond d'extraits par VOD : l'essai répartit ses N extraits."""
    if clips is None:
        return CLIPS_PER_VOD
    return max(1, math.ceil(clips / workers))


@dataclass
class Tally:
    made: int = 0
    failed: int = 0
    gone: int = 0
    vods: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add(self, made: int, failed: int, gone: int) -> None:
        with self.lock:
            self.made += made
            self.failed += failed
            self.gone += gone
            self.vods += 1


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pc-clips")
    parser.add_argument("--clips", type=int, default=None, help="essai : N extraits puis stop")
    parser.add_argument("--hours", type=float, default=9.0, help="durée maximale")
    parser.add_argument("--workers", type=int, default=None, help="VOD en parallèle")
    # Banc du 26/11/2026 sur le PC : `faster` 2,9 fois plus vite que `slow`
    # (VMAF 94,5 contre 95,4) ; l'encodeur de la Radeon (AMF) tombe à 79.
    parser.add_argument("--preset", default="faster", help="preset x264")
    args = parser.parse_args(argv)

    agent = _agent()
    agent.lower_priority()
    env, source = agent.agent_env(agent.ROOT, os.environ)
    database = agent.database_url(env, "dev")
    keys = ("R2_ACCOUNT_ID", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
    missing = [name for name in keys if not env.get(name)]
    if not database or missing:
        print(f"MANQUE dans {source.name} : base ou " + ", ".join(missing), file=sys.stderr)
        return 2

    import psycopg
    from worker import pov_clips
    from worker.storage import R2Storage

    storage = R2Storage.from_credentials(
        bucket=env["R2_BUCKET"],
        endpoint_url=f"https://{env['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        access_key_id=env["R2_ACCESS_KEY_ID"],
        secret_access_key=env["R2_SECRET_ACCESS_KEY"],
    )
    workers = args.workers or workers_for(os.cpu_count())
    limit = per_vod_limit(args.clips, workers)
    deadline = time.monotonic() + args.hours * 3600
    # L'essai prend une VOD par fil, les plus anciennes d'abord.
    with psycopg.connect(database, autocommit=True, **KEEPALIVE) as connection:
        vods = pov_clips.pending_vods(connection, workers if args.clips else 500)
    print(
        f"{platform.processor() or platform.machine()} · {os.cpu_count()} fils · "
        f"{workers} VOD à la fois · preset {args.preset} · {len(vods)} VOD en attente",
        flush=True,
    )
    tally = Tally()
    started = time.monotonic()

    def attempt(vod: str) -> pov_clips.Report:
        with psycopg.connect(database, autocommit=True, **KEEPALIVE) as own:
            return pov_clips.run(
                own,
                storage,
                vods=[vod],
                limit=limit,
                time_left=lambda: deadline - time.monotonic(),
                preset=args.preset,
            )

    def one(vod: str) -> None:
        if time.monotonic() > deadline:
            return
        while agent.gaming():
            time.sleep(60)
        try:
            report = with_retries(
                lambda: attempt(vod),
                transient=lambda error: isinstance(error, psycopg.OperationalError),
            )
        except Exception as error:  # noqa: BLE001 — une VOD perdue n'arrête pas le lot
            print(f"{vod} : abandonnée cette nuit ({type(error).__name__}: {error})", flush=True)
            return
        tally.add(report.made, report.failed, report.gone)
        hours = (time.monotonic() - started) / 3600
        print(
            f"{vod} : +{report.made} faits, {report.failed} ratés · total {tally.made} "
            f"en {hours * 60:.1f} min ({tally.made / max(hours, 1e-6):.0f}/h)",
            flush=True,
        )

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(one, vods))
    minutes = (time.monotonic() - started) / 60
    print(
        f"FIN : {tally.made} extraits, {tally.failed} ratés, {tally.gone} VOD effacées, "
        f"{tally.vods} VOD, {minutes:.1f} min",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(run())
