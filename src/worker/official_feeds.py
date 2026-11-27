"""Le flux officiel de chaque langue, pour chaque jeu : `infra/official_feeds.json`.

Item 4 de `docs/plans/multilangue-live.md`. Ibai regarde le flux espagnol,
Kameto le français : chacun se cale sur le flux de sa langue, et ce flux sur le
broadcast anglais par l'image (`cross_language.py`). Ce fichier dit quel est ce
flux.

**Rempli à la main et vérifié sur Helix, jamais deviné.** Le 15/11/2026, chaque
flux LoL a été vu exister, déclarer sa langue et avoir une VOD de la
demi-finale 2 des Worlds le 8/11. Ce que Helix ne dit pas reste en note : que
la VOD d'otplol_ est réservée aux abonnés, que le latino saute. Une chaîne dont
aucun événement n'a été vu porte `verified: null`.

Plusieurs flux d'une même langue sont permis, dans l'ordre de préférence. Un
seul est la référence de chaque jeu, et il est anglais : c'est l'horloge
commune.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

FEEDS_PATH = Path(__file__).resolve().parents[4] / "infra" / "official_feeds.json"
# Les langues suivies, décidées par le propriétaire du projet le 15/11/2026
# (décision 1 du plan multilangue) : le chinois a un flux, mais n'est pas suivi.
# Élargies le 18/11/2026 aux langues où des co-streams des Worlds ont une VOD
# publique (`docs/plans/plus-de-pov.md`). Le coréen n'est pas sur Twitch.
FOLLOWED_LANGUAGES = ("en", "fr", "es", "pt", "de", "pl", "it", "ja")


class OfficialFeed(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    login: str = Field(pattern=r"^[a-z0-9_]{2,25}$")
    language: str = Field(pattern=r"^[a-z]{2}$")
    reference: bool = False
    # Le jour où une VOD d'événement de cette chaîne a été vue sur Helix.
    verified: date | None = None
    subscriber_only_vods: bool = False
    note: str | None = None


class OfficialFeeds(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    version: int = Field(ge=1, le=1)
    games: dict[str, list[OfficialFeed]] = Field(alias="feeds", min_length=1)

    @model_validator(mode="after")
    def _one_english_reference(self) -> OfficialFeeds:
        for game, feeds in self.games.items():
            logins = [feed.login for feed in feeds]
            if len(set(logins)) != len(logins):
                raise ValueError(f"{game} : une chaîne listée deux fois")
            references = [feed for feed in feeds if feed.reference]
            if len(references) != 1:
                raise ValueError(f"{game} : {len(references)} référence(s), il en faut une")
            if references[0].language != "en":
                raise ValueError(f"{game} : la référence doit être le flux anglais")
        return self

    def reference(self, game: str) -> OfficialFeed:
        return next(feed for feed in self._feeds(game) if feed.reference)

    def in_language(self, game: str, language: str) -> list[OfficialFeed]:
        """Les flux de cette langue, dans l'ordre de préférence ; vide si aucun."""
        return [feed for feed in self._feeds(game) if feed.language == language]

    def followed(self, game: str) -> list[OfficialFeed]:
        """Les flux des autres langues suivies, référence anglaise exclue."""
        return [
            feed
            for feed in self._feeds(game)
            if not feed.reference and feed.language in FOLLOWED_LANGUAGES
        ]

    def _feeds(self, game: str) -> list[OfficialFeed]:
        if game not in self.games:
            raise KeyError(f"aucun flux officiel pour {game}")
        return self.games[game]


def load(path: Path = FEEDS_PATH) -> OfficialFeeds:
    return OfficialFeeds.model_validate(json.loads(Path(path).read_text(encoding="utf-8")))
