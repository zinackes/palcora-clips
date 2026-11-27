"""Les plafonds du worker, écrits une seule fois (règle d'audit 2026-11-19).

`MODAL_MAX_CONTAINERS` : combien de conteneurs chaque fonction Modal peut
tenir en même temps (audit H15). Un appel au-delà attend son tour au lieu
d'allumer une machine de plus.
"""

from dataclasses import dataclass
from datetime import timedelta

from worker.discovery.recordings import MAX_PARALLEL as RECORDINGS_IN_PARALLEL

MODAL_MAX_CONTAINERS: dict[str, int] = {
    # Les jobs des utilisateurs : quelques-uns à la fois, le reste attend.
    "compile_moment": 4,
    # Un passage de montage par conteneur (docs/plans/rendu-rapide.md, item 3) :
    # deux montages de 8 passages à la fois, le reste attend.
    "render_scene": 16,
    "analyze_timeline": 3,
    "preview_timeline": 4,
    # La collecte : une passe à la fois, jamais deux qui se marchent dessus.
    "monitor_jobs": 1,
    # Le rattrapage des aperçus (version 2) : un event par conteneur, quatre à la fois.
    "moment_previews": 4,
    # Les extraits gardés (docs/plans/moments-chez-nous.md) : une VOD par
    # conteneur ; huit à la fois pour le rattrapage, le reste attend.
    "pov_clips": 8,
    "pov_clips_vods": 1,
    # Le passage des extraits en accès peu fréquent (docs/plans/stockage-video.md) :
    # une seule passe à la fois, elle reprend où elle s'est arrêtée.
    "clips_storage": 1,
    # Les sous-titres des extraits (docs/plans/sous-titres.md) : un GPU par
    # conteneur ; quatre à la fois pour le rattrapage, le reste attend.
    "clip_subtitles": 4,
    # La passe de nuit des sous-titres : un seul lanceur, sans GPU.
    "nightly_subtitles": 1,
    # Les clips TikTok (docs/plans/clips-tiktok.md) : lancés à la main, par lot.
    "tiktok_clip": 4,
    "verify_vods": 1,
    "discover": 1,
    "discover_sweep": 1,
    "refresh_calendar": 1,
    "lolesports_ids": 1,
    # L'état de la chaîne (worker/health.py) : une passe à la fois.
    "pipeline_health": 1,
    "read_lolesports_league": 1,
    "youtube_replays": 1,
    "youtube_align": 1,
    "upcoming_matches": 2,
    # Un conteneur par live enregistré, au plus ce que la collecte réserve.
    "record_stream": RECORDINGS_IN_PARALLEL,
    # Les points d'entrée HTTP : courts, ils ne font que lancer ou tuer.
    "dispatch": 4,
    "cancel": 4,
}

# Audit M-W2 : erreurs de base consécutives qu'un battement de cœur tolère
# avant d'abandonner l'essai (5 s d'écart : 15 s de panne au plus).
HEARTBEAT_MAX_FAILURES = 3
# Délai de connexion à Postgres, en secondes, pour toute connexion du worker.
DB_CONNECT_TIMEOUT_S = 10
# Délai de `refresh_calendar`, en secondes. Liquipedia impose 2 s entre deux
# requêtes : une première lecture des 65 pages de `infra/schedule.json`
# (révisions, brackets, logos) a dépassé 600 s le 26/11/2026. Les passes
# suivantes ne relisent que les pages changées et restent bien en dessous.
CALENDAR_TIMEOUT_S = 1800

# Un extrait YouTube demandé par un rendu (plan `streamers-fr.md`, item 15) :
# l'agent du PC passe toutes les 15 min ; le rendu l'attend au plus 20 min,
# sous le délai de `render_scene` (1800 s), en relisant la file toutes les 20 s.
CLIP_WAIT_S = 1200.0
CLIP_POLL_S = 20.0
# L'agent tourne sur le PC du propriétaire pendant qu'il s'en sert : le
# réencodage d'un extrait (coupe pile) n'y prend que deux cœurs. Un passage
# tire d'abord quelques extraits (les `montage` attendus par un rendu passent
# en tête), puis l'audio des replays, puis le reste des extraits.
AGENT_FFMPEG_THREADS = 2
AGENT_CLIPS_BEFORE_AUDIO = 3

# Audit M-W5 : budget total d'une compilation, en secondes. Sous le délai de
# `compile_moment` (3600 s) pour échouer proprement avant que Modal ne tue.
COMPILE_BUDGET_S = 3300.0

# Audit M-W3 : jobs en file relancés au plus par passage de `monitor_jobs`.
STRANDED_JOBS_PER_PASS = 10

# Audit M-W3 : après un job lancé, la collecte légère surveille toutes les
# 15 min pendant cette durée (un rendu ou une analyse dure moins d'une heure).
JOBS_MONITORED_FOR = timedelta(hours=2)

# La publication automatique des analyses (docs/plans/v15-public.md, item 7) :
# une analyse entre seule au catalogue avec au moins 3 POV dont la confiance
# d'alignement médiane passe 0,6, et au moins un moment. Chaque passe en
# examine au plus 50 terminées depuis 7 jours et en publie au plus 5.
AUTO_PUBLISH_MIN_SOURCES = 3
AUTO_PUBLISH_MIN_CONFIDENCE = 0.6
AUTO_PUBLISH_MIN_MOMENTS = 1
AUTO_PUBLISH_CANDIDATES = 50
AUTO_PUBLISH_PER_PASS = 5
AUTO_PUBLISH_WINDOW = timedelta(days=7)

# L'alerte « match fini sans moments » (docs/plans/v15-public.md, item 8) : la
# fin d'un match s'estime à une heure et quart par partie ; 12 h après, un
# match sans moment est signalé, dans une fenêtre de 2 h (une passe de
# collecte légère), donc une seule fois.
MATCH_GAME_DURATION = timedelta(minutes=75)
MISSING_MOMENTS_AFTER = timedelta(hours=12)
MISSING_MOMENTS_SPAN = timedelta(hours=2)

# Une analyse : six heures de la VOD de référence au plus, le broadcast et
# onze co-streamers (`docs/plans/plus-de-pov.md`, item 2). Au-delà de huit, la
# mesure passe en deux vagues sur les huit cœurs ; seize n'a pas été mesuré
# contre le budget. Un montage garde, lui, huit POV au plus.
MAX_ANALYSIS_S = 6 * 3600
MAX_ANALYSIS_SOURCES = 12

# L'analyse automatique des matchs finis (docs/plans/analyses-auto.md) : un
# match s'analyse une demi-heure après sa fin estimée, s'il a été joué dans
# les 14 derniers jours (les VOD Twitch vivent 60 j). Deux analyses du compte
# de publication à la fois, une lancée par passe, 120 par mois (~0,06–0,23 $
# chacune), deux essais par match. Un co-stream compte s'il couvre la moitié
# de la fenêtre.
AUTO_ANALYZE_SETTLE = timedelta(minutes=30)
AUTO_ANALYZE_LOOKBACK = timedelta(days=14)
AUTO_ANALYZE_ACTIVE = 2
AUTO_ANALYZE_PER_PASS = 1
AUTO_ANALYZE_PER_MONTH = 120
AUTO_ANALYZE_ATTEMPTS = 2
AUTO_ANALYZE_MIN_COVERAGE = 0.5
AUTO_ANALYZE_CANDIDATES = 20

# L'extrait d'un moment publié (worker.moment_previews, version 2) : le moment
# entier, borné ici. Les moments durent 39 s en médiane, 99 s au plus (25/11).
PREVIEW_MAX_S = 120.0

# Les extraits gardés chez nous (worker.pov_clips, docs/plans/moments-chez-nous.md) :
# le moment en temps de la POV, élargi de cette marge de chaque côté pour
# qu'une recoupe du moment retrouve son extrait, et borné comme l'aperçu.
CLIP_MARGIN_S = 3.0
CLIP_MAX_S = int(PREVIEW_MAX_S + 2 * CLIP_MARGIN_S)
# L'élan de la coupe : ffmpeg lit la VOD depuis ce nombre de secondes avant
# l'extrait, parce qu'un saut dans une playlist HLS atterrit sur l'image clé
# suivante. Plus long que l'écart d'images clés le plus large vu (4,2 s).
CLIP_RUN_UP_S = 10
# Une fenêtre qui échoue (VOD illisible, trop courte) n'est retentée que ce
# nombre de fois ; au-delà, la passe ne la regarde plus.
CLIP_MAX_ATTEMPTS = 3
# Couples moment × POV lus par passe : une VOD de Bo5 en porte une centaine.
CLIPS_PER_PASS = 400

# La vérification quotidienne des VOD (worker.discovery.availability) : Helix
# accepte 100 identifiants par appel ; une passe relit ce que le site montre et
# qui n'a pas été vu depuis `VOD_RECHECK_AFTER`.
HELIX_IDS_PER_CALL = 100
VODS_VERIFIED_PER_PASS = 2000
VOD_RECHECK_AFTER = timedelta(hours=20)

# Les sous-titres des extraits (worker.clip_subtitles, docs/plans/sous-titres.md) :
# les langues proposées en plus de l'original, dans l'ordre du lecteur.
SUBTITLE_LANGUAGES: tuple[str, ...] = ("en", "fr", "es", "ko")
# Extraits traités par appel de la fonction Modal : ~1 min d'audio chacun, quelques
# secondes de GPU, plus les appels de traduction.
SUBTITLES_PER_PASS = 200
# Extraits traités en même temps sur un GPU. Profil du 27/11 (L4) : par extrait,
# ~5 s de téléchargements, ~6 s d'envois et ~8 s de traduction pour 1,2 s de
# Whisper ; à la suite, le GPU attendait. Whisper reste un à la fois.
SUBTITLE_CLIPS_AT_ONCE = 6
# La passe de nuit (accord de Mathys du 27/11) : les extraits coupés depuis deux
# jours, au plus mille (~35 min de GPU) ; ceux déjà faits ne coûtent qu'une
# lecture sur R2. Le PC coupe ~400 extraits par nuit jusqu'à mi-décembre.
SUBTITLES_NIGHTLY_HOURS = 48
SUBTITLES_NIGHTLY_LIMIT = 1000
# Le GPU de la transcription : faster-whisper turbo en float16 y passe une
# minute d'audio en quelques secondes. Délai et marge comme pour `pov_clips`.
SUBTITLES_GPU = "L4"
SUBTITLES_TIMEOUT_S = 3600
SUBTITLES_MARGIN_S = 120

# Les clips verticaux TikTok / Shorts (worker.tiktok, docs/plans/clips-tiktok.md) :
# un moment entre ces bornes, élargi de `TIKTOK_PAD_S` de chaque côté. Jamais
# de source sous `MIN_SOURCE_H` (règle de Mathys du 19/11 : 720p au moins).
TIKTOK_MIN_S = 15.0
TIKTOK_MAX_S = 25.0
TIKTOK_PAD_S = 1.0
MIN_SOURCE_H = 720


@dataclass(frozen=True)
class HealthLimits:
    """L'alerte de la chaîne (worker/health.py, audit du 27/11, P0-10) : au-delà
    de ces âges ou de ces taux, un message part ; il est rappelé toutes les
    `repeat` tant qu'il dure."""

    discovery_stale: timedelta = timedelta(hours=2)
    calendar_stale: timedelta = timedelta(hours=12)
    intake_stale: timedelta = timedelta(hours=24)
    clips_min_tries: int = 20
    clips_failure_rate: float = 0.10
    expiry_warning_days: int = 7
    analyses_failed: int = 3
    repeat: timedelta = timedelta(hours=12)


HEALTH = HealthLimits()
