"""Coupe sur un runner GitHub les extraits gardés d'une part des VOD.

Même code que le PC (`pc_clips.py`, copie de scripts/pc-agent/clips.py) et que
Modal (`worker.pov_clips.run`) : une VOD par tâche, quelques VOD à la fois.
Chaque job de la matrice prend les VOD dont l'empreinte tombe sur son numéro,
donc deux jobs ne coupent jamais la même VOD.

    python run.py --shard 3 --shards 20 --hours 5.5
    python run.py --clips 4                         # essai

Les clés viennent de l'environnement (secrets du dépôt) : DATABASE_URL,
R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, chacune en
secret, ou un `.env` collé entier dans le secret `MULTIPOV` (on n'en lit que
ces cinq clés).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import zlib
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlsplit

KEYS = ("DATABASE_URL", "R2_ACCOUNT_ID", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
# Un runner public a 4 fils ; un encodage `faster` en prend environ deux.
WORKERS = 2
PENDING_VODS = 5000


def shard_of(vods: Sequence[str], index: int, total: int) -> list[str]:
    """Les VOD de ce job : empreinte CRC32 stable d'un run à l'autre."""
    return [vod for vod in vods if zlib.crc32(vod.encode()) % total == index]


def from_env_file(text: str, keys: Sequence[str]) -> dict[str, str]:
    """Les seules `keys` d'un `.env` collé en un secret (`MULTIPOV`), le reste ignoré."""
    found: dict[str, str] = {}
    for line in text.splitlines():
        name, sep, value = line.strip().removeprefix("export ").partition("=")
        name = name.strip()
        if sep and name in keys and not name.startswith("#"):
            found[name] = value.strip().strip("'\"")
    return found


def _mask(env: dict[str, str]) -> None:
    """Les journaux d'un dépôt public sont publics : cacher l'hôte de la base."""
    host = urlsplit(env["DATABASE_URL"]).hostname
    for value in (host, env["R2_ACCOUNT_ID"], env["R2_BUCKET"]):
        if value and os.environ.get("GITHUB_ACTIONS"):
            print(f"::add-mask::{value}", flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="palcora-clips")
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--clips", type=int, default=None, help="essai : N extraits puis stop")
    parser.add_argument("--hours", type=float, default=5.5, help="durée maximale")
    parser.add_argument("--workers", type=int, default=WORKERS)
    parser.add_argument("--preset", default="faster", help="preset x264")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    pasted = from_env_file(os.environ.get("MULTIPOV", ""), KEYS)
    env = {key: os.environ.get(key) or pasted.get(key, "") for key in KEYS}
    missing = [key for key in KEYS if not env[key]]
    if missing:
        print("MANQUE dans les secrets du dépôt : " + ", ".join(missing), file=sys.stderr)
        return 2
    _mask(env)

    import psycopg
    from pc_clips import KEEPALIVE, Tally, per_vod_limit, with_retries
    from worker import pov_clips
    from worker.storage import R2Storage

    storage = R2Storage.from_credentials(
        bucket=env["R2_BUCKET"],
        endpoint_url=f"https://{env['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com",
        access_key_id=env["R2_ACCESS_KEY_ID"],
        secret_access_key=env["R2_SECRET_ACCESS_KEY"],
    )
    database = env["DATABASE_URL"]
    limit = per_vod_limit(args.clips, args.workers)
    deadline = time.monotonic() + args.hours * 3600
    with psycopg.connect(database, autocommit=True, **KEEPALIVE) as connection:
        every = pov_clips.pending_vods(connection, PENDING_VODS)
    vods = shard_of(every, args.shard, args.shards)
    if args.clips:
        vods = vods[: args.workers]
    print(
        f"job {args.shard + 1}/{args.shards} · {os.cpu_count()} fils · {args.workers} VOD à la "
        f"fois · preset {args.preset} · {len(vods)} VOD sur {len(every)} en attente",
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
        try:
            report = with_retries(
                lambda: attempt(vod),
                transient=lambda error: isinstance(error, psycopg.OperationalError),
            )
        except Exception as error:  # noqa: BLE001 — une VOD perdue n'arrête pas le lot
            print(f"{vod} : abandonnée ce run ({type(error).__name__}: {error})", flush=True)
            return
        tally.add(report.made, report.failed, report.gone)
        hours = (time.monotonic() - started) / 3600
        print(
            f"{vod} : +{report.made} faits, {report.failed} ratés · total {tally.made} "
            f"en {hours * 60:.1f} min ({tally.made / max(hours, 1e-6):.0f}/h)",
            flush=True,
        )

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        list(pool.map(one, vods))
    minutes = (time.monotonic() - started) / 60
    summary = (
        f"{tally.made} extraits, {tally.failed} ratés, {tally.gone} VOD effacées, "
        f"{tally.vods} VOD, {minutes:.1f} min"
    )
    print(f"FIN : {summary}", flush=True)
    if path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(path, "a", encoding="utf-8") as out:
            out.write(f"job {args.shard + 1}/{args.shards} : {summary}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
