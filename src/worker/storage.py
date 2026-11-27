"""Private R2/S3 object helpers and conservative render garbage collection."""

import re
from collections.abc import Callable, Collection, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID

_SEGMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}\Z")
# Un identifiant YouTube peut commencer par « - » ou « _ » (-w1v_c-qc6M) ;
# la clé reste sans point ni barre, donc sans évasion d'espace de noms.
_YOUTUBE_ID = re.compile(r"[A-Za-z0-9_-]{1,128}\Z")
_RAW_KEY = re.compile(
    r"raw/(twitch|youtube)/([A-Za-z0-9_-]{1,128})/(0|[1-9][0-9]*)-([1-9][0-9]*)\.mp4\Z"
)
# L'audio d'un live enregistré (ADR 0014) et l'état de `record_live` à côté :
# `discovery/recordings.py` forme la clé, le login y entre tel que le catalogue
# l'accepte et le début en ISO 8601 compact, sans « : ».
_RECORDING_KEY = re.compile(
    r"raw/recordings/[a-z0-9_]{2,25}/[0-9]{8}T[0-9]{6}Z\.mp4(\.state\.json)?\Z"
)
_RENDER_KEY = re.compile(
    r"renders/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.mp4\Z"
)
# Les aperçus des moments publiés (worker.moment_previews) : l'id du moment ;
# `.sm.jpg` est la vignette des grilles (plan alleger-pages).
_PREVIEW_KEY = re.compile(
    r"previews/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.(jpg|sm\.jpg|mp4)\Z"
)
# Les extraits gardés des POV (worker.pov_clips) : la VOD et sa fenêtre, comme
# `raw/`, mais sans expiration par âge (docs/plans/moments-chez-nous.md).
_CLIP_KEY = re.compile(
    r"clips/(twitch|youtube)/([A-Za-z0-9_-]{1,128})/(0|[1-9][0-9]*)-([1-9][0-9]*)\.mp4\Z"
)
# Les sous-titres d'un extrait, à côté de lui (worker.subtitles) : une piste par
# langue et le manifeste. Les langues sont celles de `subtitles.TRACKS`.
_SUBTITLE_KEY = re.compile(
    r"clips/(twitch|youtube)/([A-Za-z0-9_-]{1,128})/(0|[1-9][0-9]*)-([1-9][0-9]*)"
    r"\.(?:(orig|en|fr|es|ko)\.vtt|subs\.json)\Z"
)
_AUDIT_KEY = re.compile(
    r"renders/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    r"\.(report|manifest)\.json\Z"
)

AUDIT_NAMES: tuple[str, ...] = ("report", "manifest")
"""Pistes d'audit écrites à côté du rendu : mesures d'alignement et origines exactes."""


@dataclass(frozen=True)
class RawWindow:
    platform: str
    vod_id: str
    start_s: int
    duration_s: int

    @property
    def key(self) -> str:
        pattern = {"twitch": _SEGMENT, "youtube": _YOUTUBE_ID}.get(self.platform)
        if pattern is None or not pattern.fullmatch(self.vod_id):
            raise ValueError("Invalid source identity")
        if (
            type(self.start_s) is not int
            or self.start_s < 0
            or type(self.duration_s) is not int
            or self.duration_s <= 0
        ):
            raise ValueError("Invalid window")
        return f"raw/{self.platform}/{self.vod_id}/{self.start_s}-{self.duration_s}.mp4"


@dataclass(frozen=True)
class ClipWindow:
    """L'extrait gardé d'une POV : mêmes règles d'identité que `RawWindow`."""

    platform: str
    vod_id: str
    start_s: int
    duration_s: int

    @property
    def key(self) -> str:
        raw = RawWindow(self.platform, self.vod_id, self.start_s, self.duration_s).key
        return "clips/" + raw.removeprefix("raw/")


@dataclass(frozen=True)
class RenderKey:
    render_id: str

    @property
    def key(self) -> str:
        try:
            parsed = UUID(self.render_id)
        except (ValueError, AttributeError) as exc:
            raise ValueError("Invalid render ID") from exc
        if str(parsed) != self.render_id:
            raise ValueError("Render ID must be a canonical UUID")
        return f"renders/{self.render_id}.mp4"


@dataclass(frozen=True)
class AuditKey:
    """Piste d'audit d'un rendu : même identité, même préfixe, même cycle de vie."""

    render_id: str
    name: str

    @property
    def key(self) -> str:
        if self.name not in AUDIT_NAMES:
            raise ValueError("Invalid audit object name")
        return f"{RenderKey(self.render_id).key.removesuffix('.mp4')}.{self.name}.json"


def render_owner_key(key: str) -> str:
    """Rend la clé du rendu dont dépend un objet : lui-même ou une de ses pistes d'audit.

    Le ramassage ne raisonne que sur des rendus : une piste d'audit ne survit
    pas au rendu qu'elle décrit, et ne disparaît jamais avant lui.
    """
    audit = _AUDIT_KEY.fullmatch(key)
    if audit and AuditKey(audit.group(1), audit.group(2)).key == key:
        return RenderKey(audit.group(1)).key
    render = _RENDER_KEY.fullmatch(key)
    if render and RenderKey(render.group(1)).key == key:
        return key
    raise ValueError("Invalid R2 render object key")


# Les classes de stockage R2 (docs/plans/stockage-video.md) : standard, ou
# accès peu fréquent pour ce qui se lit rarement (10 $/To au lieu de 15,
# 0,01 $/Go relu, 30 jours minimum).
STORAGE_CLASSES = frozenset({"STANDARD", "STANDARD_IA"})
INFREQUENT = "STANDARD_IA"


def _storage_class(value: str) -> str:
    if value not in STORAGE_CLASSES:
        raise ValueError(f"Invalid R2 storage class: {value!r}")
    return value


class S3Client(Protocol):
    def upload_file(
        self, filename: str, bucket: str, key: str, ExtraArgs: dict[str, str]
    ) -> None: ...
    def download_file(self, bucket: str, key: str, filename: str) -> None: ...
    def generate_presigned_url(
        self, operation: str, Params: dict[str, str], ExpiresIn: int
    ) -> str: ...
    def list_objects_v2(self, **kwargs: object) -> dict[str, object]: ...
    def delete_object(self, **kwargs: object) -> object: ...
    def copy_object(self, **kwargs: object) -> object: ...
    def head_object(self, **kwargs: object) -> dict[str, object]: ...


@dataclass(frozen=True)
class R2Storage:
    bucket: str
    client: S3Client

    @classmethod
    def from_credentials(
        cls, *, bucket: str, endpoint_url: str, access_key_id: str, secret_access_key: str
    ) -> "R2Storage":
        from urllib.parse import urlsplit

        import boto3
        from botocore.config import Config

        parsed = urlsplit(endpoint_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or not re.fullmatch(r"[0-9a-f]{32}\.r2\.cloudflarestorage\.com", parsed.hostname)
            or parsed.port is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or parsed.username
            or parsed.password
        ):
            raise ValueError("R2 endpoint must be an HTTPS origin")
        if not bucket or not _SEGMENT.fullmatch(bucket):
            raise ValueError("Invalid R2 bucket")
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key,
            region_name="auto",
            config=Config(
                signature_version="s3v4",
                retries={"mode": "legacy", "total_max_attempts": 5},
                connect_timeout=10,
                read_timeout=60,
            ),
        )
        return cls(bucket, client)

    @staticmethod
    def validate_key(key: str) -> str:
        raw = _RAW_KEY.fullmatch(key)
        if raw:
            if int(raw.group(4)) <= 0:
                raise ValueError("Invalid raw duration")
            if (
                RawWindow(raw.group(1), raw.group(2), int(raw.group(3)), int(raw.group(4))).key
                == key
            ):
                return key
        if _RECORDING_KEY.fullmatch(key):
            return key
        render = _RENDER_KEY.fullmatch(key)
        if render and RenderKey(render.group(1)).key == key:
            return key
        audit = _AUDIT_KEY.fullmatch(key)
        if audit and AuditKey(audit.group(1), audit.group(2)).key == key:
            return key
        if _PREVIEW_KEY.fullmatch(key):
            return key
        clip = _CLIP_KEY.fullmatch(key)
        if clip and (
            ClipWindow(clip.group(1), clip.group(2), int(clip.group(3)), int(clip.group(4))).key
            == key
        ):
            return key
        subtitle = _SUBTITLE_KEY.fullmatch(key)
        if subtitle and key.startswith(
            ClipWindow(
                subtitle.group(1), subtitle.group(2), int(subtitle.group(3)), int(subtitle.group(4))
            ).key.removesuffix(".mp4")
            + "."
        ):
            return key
        raise ValueError("Invalid R2 object key")

    def upload(self, path: Path, key: str, *, storage_class: str | None = None) -> None:
        validated = self.validate_key(key)
        content_type = (
            "application/json"
            if validated.endswith(".json")
            else "text/vtt; charset=utf-8"
            if validated.endswith(".vtt")
            else "image/jpeg"
            if validated.endswith(".jpg")
            else "video/mp4"
        )
        extra = {"ContentType": content_type}
        if storage_class is not None:
            extra["StorageClass"] = _storage_class(storage_class)
        self.client.upload_file(str(path), self.bucket, validated, ExtraArgs=extra)

    def storage_class_of(self, key: str) -> str:
        """La classe d'un objet ; R2 ne l'annonce pas pour le standard."""
        head = self.client.head_object(Bucket=self.bucket, Key=self.validate_key(key))
        return str(head.get("StorageClass") or "STANDARD")

    def set_storage_class(self, key: str, storage_class: str) -> None:
        """Change la classe d'un extrait déjà rangé : copie sur place, mêmes octets."""
        validated = self.validate_key(key)
        if not _CLIP_KEY.fullmatch(validated):
            raise ValueError("Only clips change storage class")
        self.client.copy_object(
            Bucket=self.bucket,
            Key=validated,
            CopySource={"Bucket": self.bucket, "Key": validated},
            StorageClass=_storage_class(storage_class),
            MetadataDirective="COPY",
        )

    def exists(self, key: str) -> bool:
        """L'objet est-il là ? Un 404 dit non, toute autre erreur remonte."""
        from botocore.exceptions import ClientError

        try:
            self.client.head_object(Bucket=self.bucket, Key=self.validate_key(key))
        except ClientError as error:
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404 or error.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
                return False
            raise
        return True

    def download(self, key: str, path: Path) -> None:
        self.client.download_file(self.bucket, self.validate_key(key), str(path))

    def signed_url(self, key: str, expires_s: int = 900) -> str:
        if type(expires_s) is not int or not 1 <= expires_s <= 900:
            raise ValueError("Signed URL expiry must be between 1 and 900 seconds")
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": self.validate_key(key)},
            ExpiresIn=expires_s,
        )

    def render_objects(self) -> Iterator[tuple[str, datetime]]:
        token: str | None = None
        while True:
            kwargs: dict[str, object] = {"Bucket": self.bucket, "Prefix": "renders/"}
            if token:
                kwargs["ContinuationToken"] = token
            page = self.client.list_objects_v2(**kwargs)
            for entry in page.get("Contents", []):
                if isinstance(entry, dict):
                    key, modified = entry.get("Key"), entry.get("LastModified")
                    if isinstance(key, str) and isinstance(modified, datetime):
                        yield key, modified
            if not page.get("IsTruncated"):
                return
            next_token = page.get("NextContinuationToken")
            if not isinstance(next_token, str) or not next_token or next_token == token:
                raise RuntimeError("Invalid R2 listing continuation token")
            token = next_token


# Audit H3 : en dessous, le seau est trop petit pour juger la base.
GC_GUARD_MIN_RENDERS = 10


def collect_orphan_renders(
    storage: R2Storage,
    referenced_keys: Callable[[], tuple[Collection[str], Collection[str]]],
    delete_if_safe: Callable[[str, Callable[[], None]], bool],
    *,
    now: datetime | None = None,
    grace: timedelta = timedelta(days=1),
    apply: bool = False,
    known_keys: Callable[[], Collection[str]] | None = None,
) -> list[str]:
    """List candidates, optionally delete after a fresh DB-backed eligibility check.

    The callback owns both the fresh DB check and deletion while holding the
    same coordination lock used by uploads/finalization. It must fail closed.

    `known_keys` lists every render the database has ever recorded, deleted
    or not. Dev and prod share one bucket (audit H3): when the database knows
    fewer than half of the bucket's renders, it is the other environment's,
    and deleting is refused.
    """
    current = now or datetime.now(UTC)
    if current.tzinfo is None or grace <= timedelta(0):
        raise ValueError("A timezone-aware current time and positive grace are required")
    live, inflight = referenced_keys()
    protected = set(live) | set(inflight)
    objects: list[tuple[str, str, datetime]] = []
    for key, modified in storage.render_objects():
        try:
            objects.append((key, render_owner_key(storage.validate_key(key)), modified))
        except ValueError:
            continue
    if apply and known_keys is not None:
        owners = {owner for _key, owner, _modified in objects}
        known = len(owners & set(known_keys()))
        if len(owners) >= GC_GUARD_MIN_RENDERS and known * 2 < len(owners):
            raise RuntimeError(
                f"refus : la base ne connaît que {known} rendus sur {len(owners)} "
                "dans le seau, mauvaise base ?"
            )
    removed: list[str] = []
    for key, owner, modified in objects:
        if modified.tzinfo is None or modified > current - grace or owner in protected:
            continue
        if apply:
            if not delete_if_safe(
                key,
                lambda key=key: storage.client.delete_object(Bucket=storage.bucket, Key=key),
            ):
                continue
        removed.append(key)
    return removed
