"""Métadonnées Helix d'une VOD Twitch : `created_at` et `duration`.

`created_at` est l'ancrage horloge de tout le pipeline : c'est lui qui place le
même instant réel dans chaque VOD et qui borne la fenêtre de recherche du
fingerprint (`align/clock.py`). Il est donc toujours rendu en UTC *aware* — un
datetime naïf ferait une erreur d'ancrage silencieuse de plusieurs heures.

Un app access token (client credentials) suffit ici. `muted_segments` exige un
user token et reste hors périmètre V1 (docs/research/apis.md §Twitch Helix).
"""

from __future__ import annotations

import os
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse

import httpx

HELIX_VIDEOS_URL = "https://api.twitch.tv/helix/videos"
OAUTH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"

# Allowlist d'hôtes : le worker ne va que là où on l'a autorisé (ADR 0003).
_ALLOWED_HOSTS = frozenset({"twitch.tv", "www.twitch.tv"})

_VOD_PATH_RE = re.compile(r"^/videos/(\d+)/?$")
_DURATION_RE = re.compile(r"^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$")

# Marge de sécurité sur l'expiration du token : on le renouvelle avant l'heure.
_TOKEN_EXPIRY_MARGIN_S = 60.0
# Helix plafonne par minute (429) et flanche parfois (5xx) : trois essais en
# tout, jamais plus de 10 s d'attente, pour qu'un passage du cron ne s'arrête
# pas sur un hoquet ni ne dorme une minute.
MAX_ATTEMPTS = 3
MAX_RETRY_WAIT_S = 10.0
_RETRIED = frozenset({429, 500, 502, 503, 504})


class VodNotFound(LookupError):
    """La VOD n'est pas (ou plus) servie par Helix — expirée, supprimée ou privée."""


@dataclass(frozen=True)
class VodInfo:
    """Ce qu'on retient d'une VOD. `created_at` est UTC aware, `duration_s` en secondes."""

    id: str
    created_at: datetime
    duration_s: int
    user_login: str
    url: str


def parse_vod_url(url: str) -> str:
    """Rend le `vod_id` d'une URL de VOD Twitch. Tolère `?t=1h2m3s`.

    Tout le reste est refusé : autre hôte, autre chemin, ou http en clair.
    """
    if url.isdigit():
        return url

    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in _ALLOWED_HOSTS:
        raise ValueError(f"URL non autorisée (VOD Twitch en https uniquement) : {url}")

    match = _VOD_PATH_RE.match(parsed.path)
    if match is None:
        raise ValueError(f"URL sans identifiant de VOD : {url}")
    return match.group(1)


def parse_helix_duration(text: str) -> int:
    """Convertit une durée Helix (`8h8m22s`, `45m`, `3h`) en secondes."""
    match = _DURATION_RE.match(text)
    if match is None or not any(match.groups()):
        raise ValueError(f"durée Helix illisible : {text!r}")
    hours, minutes, seconds = (int(group or 0) for group in match.groups())
    return hours * 3600 + minutes * 60 + seconds


def _parse_created_at(text: str) -> datetime:
    """Helix rend du RFC 3339 suffixé `Z` ; on le veut UTC aware, toujours."""
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError(f"created_at sans fuseau, ancrage horloge impossible : {text!r}")
    return moment


class TwitchClient:
    """Client Helix minimal : un app token mis en cache, et `GET /videos`."""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        transport: httpx.BaseTransport | None = None,
        timeout: float = 10.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._sleep = sleep
        self._clock = clock
        self._client_id = client_id
        self._client_secret = client_secret
        self._http = httpx.Client(transport=transport, timeout=timeout)
        self._token: str | None = None
        self._token_expires_at = 0.0
        # Appels Helix faits par ce client, hors token : ce que le cron de
        # découverte coûte en quota, mesuré plutôt qu'estimé.
        self.calls = 0

    @classmethod
    def from_env(cls) -> TwitchClient:
        """Construit le client depuis l'environnement (cf. `.env.example`)."""
        missing = [
            name
            for name in ("TWITCH_CLIENT_ID", "TWITCH_CLIENT_SECRET")
            if not os.environ.get(name)
        ]
        if missing:
            raise RuntimeError(f"variable(s) d'environnement manquante(s) : {', '.join(missing)}")
        return cls(os.environ["TWITCH_CLIENT_ID"], os.environ["TWITCH_CLIENT_SECRET"])

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> TwitchClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _app_token(self) -> str:
        """Rend un app token valide, en le renouvelant seulement quand il expire."""
        if self._token is not None and time.monotonic() < self._token_expires_at:
            return self._token

        response = self._http.post(
            OAUTH_TOKEN_URL,
            data={
                "client_id": self._client_id,
                "client_secret": self._client_secret,
                "grant_type": "client_credentials",
            },
        )
        response.raise_for_status()
        payload = response.json()
        self._token = str(payload["access_token"])
        expires_in = float(payload.get("expires_in", 3600))
        self._token_expires_at = time.monotonic() + expires_in - _TOKEN_EXPIRY_MARGIN_S
        return self._token

    def get(self, url: str, params: dict[str, str | int | list[str]]) -> dict[str, object]:
        """Un GET Helix authentifié. Le seul endroit qui connaît le token.

        Tout appel Helix passe par ici : ajouter un endpoint ne doit pas vouloir
        dire redupliquer l'en-tête et le cycle de vie du token.
        """
        renewed = False
        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.calls += 1
            try:
                response = self._http.get(
                    url,
                    params=params,
                    headers={
                        "Client-Id": self._client_id,
                        "Authorization": f"Bearer {self._app_token()}",
                    },
                )
            except httpx.TransportError:
                if attempt == MAX_ATTEMPTS:
                    raise
                self._sleep(0.5 * attempt)
                continue
            if response.status_code == 401 and not renewed:
                # Un app token révoqué ou expiré avant l'heure : un seul renouvellement.
                self._token = None
                renewed = True
                continue
            if response.status_code in _RETRIED and attempt < MAX_ATTEMPTS:
                self._sleep(self._retry_wait(response, attempt))
                continue
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        response.raise_for_status()
        return {}

    def _retry_wait(self, response: httpx.Response, attempt: int) -> float:
        """Jusqu'au `Ratelimit-Reset` d'un 429 (epoch en secondes), 1 s sans lui ;
        une attente croissante pour un 5xx. Jamais plus de `MAX_RETRY_WAIT_S`."""
        if response.status_code == 429:
            try:
                wait = float(response.headers["Ratelimit-Reset"]) - self._clock()
            except (KeyError, ValueError):
                wait = 1.0
        else:
            wait = 0.5 * attempt
        return min(max(wait, 0.0), MAX_RETRY_WAIT_S)

    def vod_info(self, url_or_id: str) -> VodInfo:
        """Métadonnées d'une VOD, depuis son URL ou son identifiant."""
        vod_id = parse_vod_url(url_or_id)

        # Helix rend 200 avec `data: []` pour une VOD expirée : sans cette garde
        # on lirait un index vide et l'erreur remonterait loin de sa cause.
        entries = self.get(HELIX_VIDEOS_URL, {"id": vod_id}).get("data", [])
        if not entries:
            raise VodNotFound(f"aucune VOD {vod_id} — expirée, supprimée ou privée ?")

        entry = entries[0]
        return VodInfo(
            id=str(entry["id"]),
            created_at=_parse_created_at(entry["created_at"]),
            duration_s=parse_helix_duration(entry["duration"]),
            user_login=str(entry["user_login"]),
            url=str(entry["url"]),
        )
