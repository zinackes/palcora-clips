"""Trouver les VOD qui couvrent un instant donné, sans ouvrir un navigateur.

Monter un jeu de test veut dire trouver les VOD de plusieurs chaînes ayant
diffusé le même moment. À la main c'est du parcours de Twitch, ce qui suppose
d'y avoir accès — pas le cas depuis un poste bloqué, ni depuis une sandbox
d'agent. Helix, lui, répond depuis un runner.

Le tri utile n'est pas « qui a streamé ce jeu » mais « qui était en train de
diffuser à l'instant T » : une VOD couvre T si
`created_at <= T <= created_at + duration`. Pour chaque candidate on en déduit
directement le `moment_ts` à écrire dans le manifest, c'est-à-dire la position
de T dans CETTE vod — le même calcul que le prior d'horloge de `align/clock.py`,
validé à 6 et 7 s près sur le jeu réel.

**Pourquoi on ne cherche pas directement dans les VOD.** Ce serait la voie
évidente — les streams visés sont finis, ce sont des VOD. Mais `/videos`
n'accepte que `id`, `user_id` ou `game_id` : aucune recherche par texte, donc
aucun moyen de demander « les VOD dont le titre contient RLCS ». Il n'existe
pas d'endpoint Helix de recherche de VOD. D'où le détour : trouver les
**chaînes** par texte, puis leurs VOD par `user_id`.

Les limites à connaître avant de s'y fier :

- `/videos` ne filtre pas par fenêtre horaire (docs/research/apis.md), d'où la
  pagination puis le filtrage côté client.
- `/search/channels` compare au nom et au titre **actuels** d'une chaîne, pas
  au titre qu'avait le stream au moment visé. Un co-streamer qui a changé de
  titre depuis sort donc des résultats : c'est une vraie limite, et elle plaide
  pour lancer la découverte **pendant ou juste après** l'event.
- les deux plafonds, pages de VOD et nombre de chaînes, tronquent. Ils sont
  signalés (`Search.inconclusive`) ; ne jamais lire une liste vide sans les
  regarder.

**Attention à ce que la troncature a déjà fait croire.** Trois vrais runs ont
affiché « 100 chaîne(s) examinée(s) » — soit exactement le plafond d'alors, pas
un total — puis « aucune VOD ne couvre cet instant ». On en a conclu que les
watch parties de la veille étaient devenues introuvables. C'était une
sur-interprétation : la recherche s'était arrêtée au plafond, et Twitch classe
les chaînes en direct d'abord, donc ces 100 étaient surtout les streams du jour.
Le plafond a été relevé et sa troncature est désormais signalée ; que le
changement de titre suffise à rendre une chaîne introuvable reste à mesurer.

Chercher une chaîne connue par son nom marche à tout moment, et rend
`created_at` et `moment_ts` prêts pour le manifest. C'est l'usage le plus sûr.

Et ce qu'elle ne dira jamais : si le co-streamer garde l'audio du broadcast.
C'est le critère éliminatoire du runbook, il s'écoute.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from worker.ingest.twitch import (
    HELIX_VIDEOS_URL,
    TwitchClient,
    _parse_created_at,
    parse_helix_duration,
)

HELIX_GAMES_URL = "https://api.twitch.tv/helix/games"
HELIX_STREAMS_URL = "https://api.twitch.tv/helix/streams"
HELIX_SEARCH_CHANNELS_URL = "https://api.twitch.tv/helix/search/channels"
HELIX_USERS_URL = "https://api.twitch.tv/helix/users"

PAGE_SIZE = 100
# Mesuré : 10 pages ne remontent que 2 h 30 dans une catégorie fréquentée
# comme Rocket League. Chercher un instant d'hier en demandait une soixantaine.
MAX_PAGES = 80
# Mesuré : trois vrais runs ont tous affiché exactement 100 chaînes, c'est-à-dire
# ce plafond et pas un total. Chaque chaîne coûte un appel /videos, donc 400 tient
# largement dans les ~800 points/min de Helix et dans le timeout du workflow.
MAX_CHANNELS = 400

# Un stream plus ancien que ça ne couvrira pas T : c'est la borne qui permet
# d'arrêter de paginer. Elle était à 30 h, et un vrai run l'a démentie — la VOD
# d'`okiedokiebear` démarre 46 h avant l'instant et le couvre quand même
# (moment_ts 45:59:49). Trié par date, la pagination se serait arrêtée avant de
# l'atteindre. 72 h laisse de la marge aux marathons sans coûter grand-chose :
# la borne ne fait qu'autoriser des pages en plus, elle n'en impose aucune.
MAX_STREAM_HOURS = 72

# Tris et fenêtres acceptés par /videos. `time` rend les VOD les plus récentes —
# donc, dans une grosse catégorie, des petits streams au hasard : mesuré, 160
# joueurs de Rocket League et aucune watch party. `views` rend les plus
# regardées, ce qui est exactement là où se range une watch party d'event.
# `period` borne la fenêtre côté serveur, seul moyen d'y arriver puisque
# /videos ne filtre pas par heure.
SORTS = frozenset({"time", "trending", "views"})
PERIODS = frozenset({"day", "week", "month", "all"})


class GameNotFound(LookupError):
    """Aucune catégorie Twitch de ce nom."""


@dataclass(frozen=True)
class Candidate:
    """Une VOD qui couvre l'instant visé, prête à copier dans le manifest."""

    vod_id: str
    user_login: str
    url: str
    created_at: datetime
    duration_s: int
    moment_s: float
    live: bool = False

    @property
    def moment_ts(self) -> str:
        """`moment_ts` du manifest : la position de l'instant dans CETTE vod."""
        total = int(self.moment_s)
        return f"{total // 3600:02d}:{total % 3600 // 60:02d}:{total % 60:02d}"


def game_id(client: TwitchClient, name: str) -> str:
    """Identifiant de catégorie Twitch, par nom exact."""
    entries = client.get(HELIX_GAMES_URL, {"name": name}).get("data", [])
    if not entries:
        raise GameNotFound(f"aucune catégorie Twitch nommée {name!r}")
    return str(entries[0]["id"])


def _videos_page(
    client: TwitchClient, params: dict[str, str | int]
) -> tuple[list[dict[str, object]], str | None]:
    payload = client.get(HELIX_VIDEOS_URL, params)
    data = payload.get("data", [])
    pagination = payload.get("pagination", {})
    cursor = pagination.get("cursor") if isinstance(pagination, dict) else None
    entries = data if isinstance(data, list) else []
    return entries, str(cursor) if cursor else None


def collect_videos(
    client: TwitchClient,
    *,
    game: str | None = None,
    user_id: str | None = None,
    stop_before: datetime | None = None,
    max_pages: int = MAX_PAGES,
    sort: str = "time",
    period: str | None = None,
) -> tuple[list[dict[str, object]], bool]:
    """Rassemble `/videos` page par page, du plus récent au plus ancien.

    Rend aussi un drapeau de troncature. Il n'est pas décoratif : sans lui,
    s'arrêter sur le plafond de pages et ne rien trouver est indiscernable
    d'avoir vraiment tout regardé — et l'outil annonce « aucune VOD » alors
    qu'il a juste cessé de chercher. C'est arrivé au premier vrai run.

    `stop_before` arrête la pagination dès qu'on descend sous cette date : les
    résultats étant triés par date décroissante, tout ce qui suit est plus vieux
    encore et ne peut plus couvrir l'instant visé. S'arrêter comme ça, c'est
    avoir tout vu ; s'arrêter sur `max_pages`, non.

    Cet arrêt ne vaut QUE pour `sort="time"`. Trié par vues, une VOD ancienne
    peut précéder une récente : couper dessus ferait rater ce qui suit. On
    borne alors la recherche par `period` côté serveur, et la troncature se
    dit franchement plutôt que de se déguiser en « j'ai tout vu ».
    """
    if sort not in SORTS:
        raise ValueError(f"tri inconnu : {sort!r} (attendu {', '.join(sorted(SORTS))})")
    if period is not None and period not in PERIODS:
        raise ValueError(f"période inconnue : {period!r} (attendu {', '.join(sorted(PERIODS))})")
    params: dict[str, str | int | list[str]] = {
        "first": PAGE_SIZE,
        "type": "archive",
        "sort": sort,
    }
    if period is not None:
        params["period"] = period
    if game is not None:
        params["game_id"] = game
    if user_id is not None:
        params["user_id"] = user_id

    collected: list[dict[str, object]] = []
    for _ in range(max_pages):
        entries, cursor = _videos_page(client, dict(params))
        if not entries:
            return collected, False
        collected.extend(entries)
        if stop_before is not None and sort == "time":
            oldest = _parse_created_at(str(entries[-1]["created_at"]))
            if oldest < stop_before:
                return collected, False
        if cursor is None:
            return collected, False
        params["after"] = cursor
    return collected, True


def search_channels(
    client: TwitchClient, query: str, *, limit: int = MAX_CHANNELS
) -> tuple[list[dict[str, object]], bool]:
    """Chaînes dont le nom ou le titre de stream correspond à `query`.

    C'est l'entrée qui compte pour monter un jeu de test. Chercher par
    catégorie de jeu renvoie les gens qui *jouent* — mesuré sur un vrai run :
    160 joueurs de Rocket League et pas une seule watch party. Les
    co-streamers d'un event, eux, en mettent le nom dans leur titre.

    Rend aussi un drapeau de troncature, pour la même raison que
    `collect_videos` : s'arrêter sur le plafond et ne rien trouver ne se corrige
    pas comme avoir vraiment tout regardé. Trois vrais runs ont affiché « 100
    chaîne(s) examinée(s) » — le plafond exactement — et conclu « aucune VOD ne
    couvre cet instant ». On en a déduit à tort que les watch parties étaient
    devenues introuvables le lendemain.
    """
    found: list[dict[str, object]] = []
    params: dict[str, str | int | list[str]] = {"query": query, "first": PAGE_SIZE}
    while True:
        payload = client.get(HELIX_SEARCH_CHANNELS_URL, params)
        entries = payload.get("data", [])
        if not isinstance(entries, list) or not entries:
            return found[:limit], len(found) > limit
        found.extend(entries)
        pagination = payload.get("pagination", {})
        cursor = pagination.get("cursor") if isinstance(pagination, dict) else None
        if not cursor:
            return found[:limit], len(found) > limit
        if len(found) >= limit:
            # Un curseur de plus alors qu'on est au plafond : il y en avait d'autres.
            return found[:limit], True
        params["after"] = str(cursor)


def covers(entry: dict[str, object], instant: datetime) -> float | None:
    """Position de `instant` dans cette VOD, ou None si elle ne le couvre pas."""
    created_at = _parse_created_at(str(entry["created_at"]))
    duration_s = parse_helix_duration(str(entry["duration"]))
    offset_s = (instant - created_at).total_seconds()
    return offset_s if 0 <= offset_s <= duration_s else None


def live_logins(client: TwitchClient, logins: Sequence[str]) -> set[str]:
    """Chaînes actuellement en direct, parmi celles passées.

    Une VOD dont la chaîne diffuse encore n'est pas finalisée : sa playlist
    n'est pas fermée, ffmpeg ne peut pas y chercher une position et se colle au
    bord du direct. Autant le dire ici plutôt qu'après 30 min de runner.

    `/streams` attend des paramètres `user_login` **répétés**, pas une liste
    séparée par des virgules, et en accepte 100 par appel.
    """
    live: set[str] = set()
    for start in range(0, len(logins), PAGE_SIZE):
        chunk = list(logins[start : start + PAGE_SIZE])
        if not chunk:
            continue
        payload = client.get(HELIX_STREAMS_URL, {"first": PAGE_SIZE, "user_login": chunk})
        for entry in payload.get("data", []) or []:
            if isinstance(entry, dict):
                live.add(str(entry.get("user_login", "")))
    return live


@dataclass(frozen=True)
class Search:
    """Le résultat d'une recherche, avec de quoi juger ce qu'elle vaut."""

    candidates: list[Candidate]
    truncated: bool = False
    channels_scanned: int = 0
    channels_truncated: bool = False

    @property
    def inconclusive(self) -> bool:
        """Vrai quand une liste vide veut dire « j'ai cessé de chercher »."""
        return self.truncated or self.channels_truncated


def find_candidates(
    client: TwitchClient,
    instant: datetime,
    *,
    game: str | None = None,
    query: str | None = None,
    user_ids: Sequence[str] = (),
    max_pages: int = MAX_PAGES,
    max_channels: int = MAX_CHANNELS,
    check_live: bool = True,
    sort: str = "time",
    period: str | None = None,
) -> Search:
    """VOD couvrant `instant`, par titre de stream, catégorie et/ou chaînes imposées.

    `query` cherche dans les noms de chaînes et les titres. `game` parcourt une
    catégorie, et son `sort` y décide tout : `time` rend les derniers streams —
    des petits comptes au hasard — quand `views` rend les plus regardés, où se
    trouvent les watch parties. `period` borne la fenêtre côté serveur.

    Les candidates sont rendues triées par heure de démarrage : c'est l'ordre
    dans lequel leurs `moment_ts` décroissent, donc le plus lisible pour choisir.
    """
    if instant.tzinfo is None:
        raise ValueError("instant sans fuseau : l'ancrage horloge serait faux")
    if sort not in SORTS:
        raise ValueError(f"tri inconnu : {sort!r} (attendu {', '.join(sorted(SORTS))})")
    stop_before = instant - timedelta(hours=MAX_STREAM_HOURS)

    channel_ids: list[str] = list(user_ids)
    scanned = 0
    channels_truncated = False
    if query is not None:
        channels, channels_truncated = search_channels(client, query, limit=max_channels)
        for channel in channels:
            channel_id = str(channel.get("id", ""))
            if channel_id and channel_id not in channel_ids:
                channel_ids.append(channel_id)
        scanned = len(channel_ids)

    entries: list[dict[str, object]] = []
    truncated = False
    if game is not None:
        # `sort`/`period` ne valent que pour la catégorie : c'est là qu'il y a
        # un volume à trier. Une chaîne a peu de VOD, l'ordre chronologique y
        # reste le bon et garde l'arrêt anticipé par date.
        page, cut = collect_videos(
            client,
            game=game_id(client, game),
            stop_before=stop_before,
            max_pages=max_pages,
            sort=sort,
            period=period,
        )
        entries.extend(page)
        truncated = truncated or cut
    for channel_id in channel_ids:
        # Une chaîne a peu de VOD : la pagination ne pose ici aucun problème de
        # volume, contrairement à la recherche par catégorie.
        page, cut = collect_videos(
            client, user_id=channel_id, stop_before=stop_before, max_pages=max_pages
        )
        entries.extend(page)
        truncated = truncated or cut

    seen: set[str] = set()
    found: list[Candidate] = []
    for entry in entries:
        vod_id = str(entry["id"])
        if vod_id in seen:
            continue
        seen.add(vod_id)
        offset_s = covers(entry, instant)
        if offset_s is None:
            continue
        found.append(
            Candidate(
                vod_id=vod_id,
                user_login=str(entry["user_login"]),
                url=str(entry["url"]),
                created_at=_parse_created_at(str(entry["created_at"])),
                duration_s=parse_helix_duration(str(entry["duration"])),
                moment_s=offset_s,
            )
        )

    if check_live and found:
        live = live_logins(client, [c.user_login for c in found])
        found = [replace(c, live=c.user_login in live) for c in found]

    return Search(
        candidates=sorted(found, key=lambda c: c.created_at),
        truncated=truncated,
        channels_scanned=scanned,
        channels_truncated=channels_truncated,
    )
