# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Instagram Monitor — a Python script that scrapes Instagram Reels from specified influencer accounts via the Apify API, stores them in a SQLite database, and sends Telegram notifications when new content is found.

## Commands

```bash
# Install dependencies (no requirements.txt; only two external packages)
pip install apify-client requests

# Run the monitor
python main.py

# Debug Apify API response format
python debug_apify.py

# Inspect database schema and sample data
python inspect_db.py
```

## Required Environment Variables

Set in `.env` (git-ignored):
- `APIFY_API_KEY` — Apify authentication token
- `TELEGRAM_BOT_TOKEN` — Telegram Bot API token
- `TELEGRAM_CHAT_ID` — Target chat/user ID for notifications

The project uses a custom `.env` parser (`_load_env_file()`) instead of python-dotenv.

## Architecture

**Single-script design** in `main.py` with private helper functions:

```
Load .env → Read influencers.txt → Init SQLite DB
  → Call Apify scraper → Filter video posts → Save to DB → Telegram alert
```

Key functions in `main.py`:
- `_load_env_file()` — custom .env parser using `os.environ.setdefault()`
- `_read_influencers()` — reads usernames from `influencers.txt` (one per line)
- `_init_db()` — creates/migrates the `posts` table with auto-migration for new columns
- `_run_apify_scraper()` — calls the Apify Instagram scraper actor
- `_extract_post_fields()` — normalizes Apify JSON into a dict; handles carousel posts (`childPosts`) and multiple username field names
- `_save_posts()` — INSERT OR IGNORE with UNIQUE(username, url) deduplication
- `_send_telegram_message()` — sends via `requests` to Telegram Bot API
- `_to_iso_utc()` — converts unix timestamps, ISO strings, and numeric strings to ISO-8601 UTC

**Filtering logic**: Only video-type posts are kept; max 2 per influencer per run.

## Database

SQLite file `monitor.db` with a single `posts` table. Schema includes columns for future AI analysis (`transcript`, `ai_label`, `ai_score`, `ai_summary`, `ai_reason`, `ai_ran_at`) that are not yet populated.

## Notes

- Requires Python 3.9+ (type hints, walrus operator)
- Print statements and comments are in Portuguese
- `influencers.txt` and `monitor.db` are git-ignored
