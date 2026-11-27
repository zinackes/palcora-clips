# palcora-clips

Cuts the kept per-POV moment clips of Palcora on GitHub Actions (one Twitch VOD
passage per POV, stored in R2 before Twitch deletes the VOD).

This repository is **generated** from the main repository
(`scripts/clips-repo/build.py`): do not edit it by hand, regenerate and push.

- `.github/workflows/clips.yml`: every 6 hours and on demand, a job matrix;
  each job takes its share of the VODs (`run.py --shard i --shards N`).
- Required Actions secrets: `DATABASE_URL`, `R2_ACCOUNT_ID`, `R2_BUCKET`,
  `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`. No secret lives in the code.
- Height: up to 1080p, as offered by the source.
