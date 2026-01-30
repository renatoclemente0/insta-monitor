from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from apify_client import ApifyClient

from classifier import classify_content
from transcriber import transcribe_video
from telegram_reporter import send_analysis_report

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------

logger = logging.getLogger("main")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    logger.addHandler(_handler)
    logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

DB_PATH = "monitor.db"
INFLUENCERS_PATH = "influencers.txt"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_env_file(path: str = ".env") -> None:
    """Carrega arquivo .env simples (KEY=VALUE) para os.environ."""
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _read_influencers(path: str = INFLUENCERS_PATH) -> List[str]:
    """Le usernames do arquivo de influencers (um por linha)."""
    if not os.path.exists(path):
        raise FileNotFoundError("Arquivo '{}' nao encontrado.".format(path))

    usernames = []  # type: List[str]
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            u = raw_line.strip()
            if not u or u.startswith("#"):
                continue
            if u.startswith("@"):
                u = u[1:]
            usernames.append(u)

    if not usernames:
        raise ValueError("Nenhum perfil valido encontrado em '{}'.".format(path))
    return usernames


def _init_db(db_path: str = DB_PATH) -> None:
    """Cria/migra a tabela posts com todas as colunas necessarias."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                url TEXT NOT NULL,
                caption TEXT,
                likes INTEGER,
                timestamp TEXT,
                scraped_at TEXT NOT NULL,
                UNIQUE(username, url)
            )
            """
        )

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_username ON posts(username)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_posts_timestamp ON posts(timestamp)"
        )

        def add_column_if_missing(column_name: str, column_type: str) -> None:
            cur = conn.execute("PRAGMA table_info(posts)")
            existing = {row[1] for row in cur.fetchall()}
            if column_name not in existing:
                conn.execute(
                    "ALTER TABLE posts ADD COLUMN {} {}".format(
                        column_name, column_type
                    )
                )

        add_column_if_missing("media_url", "TEXT")
        add_column_if_missing("transcript", "TEXT")
        add_column_if_missing("ai_label", "TEXT")
        add_column_if_missing("ai_score", "INTEGER")
        add_column_if_missing("ai_summary", "TEXT")
        add_column_if_missing("ai_reason", "TEXT")
        add_column_if_missing("ai_ran_at", "TEXT")
        # Colunas para classificacao politica
        add_column_if_missing("analysis_json", "TEXT")
        add_column_if_missing("analyzed_at", "TEXT")
        add_column_if_missing("classifier_version", "TEXT")

        conn.commit()


def _to_iso_utc(value: Any) -> Optional[str]:
    """Converte diferentes formatos comuns em ISO-8601 UTC."""
    if value is None:
        return None

    if isinstance(value, (int, float)):
        v = float(value)
        if v > 10_000_000_000:
            v = v / 1000.0
        dt = datetime.fromtimestamp(v, tz=timezone.utc)
        return dt.isoformat()

    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.isdigit():
            return _to_iso_utc(int(s))
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return s

    return str(value)


def _extract_post_fields(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Extrai campos do post. Retorna None se nao for Reel (Video direto).
    Descarta Sidecar/Carousel — aceita APENAS type == "Video".
    """
    if item.get("type") != "Video":
        return None

    media_url = item.get("videoUrl")
    if not media_url:
        return None

    username = (
        item.get("ownerUsername")
        or item.get("username")
        or item.get("ownerFullName")
        or item.get("inputUrl", "").split("/")[-1]
    )

    url = item.get("url")
    caption = item.get("caption")

    likes = item.get("likesCount")
    try:
        likes = int(likes) if likes is not None else None
    except Exception:
        likes = None

    timestamp = _to_iso_utc(item.get("timestamp"))

    return {
        "username": username,
        "url": url,
        "caption": caption,
        "likes": likes,
        "timestamp": timestamp,
        "media_url": media_url,
    }


def _save_posts(
    items: List[Dict[str, Any]], db_path: str = DB_PATH
) -> List[Dict[str, Any]]:
    """
    Salva posts no SQLite, evitando duplicados por (username, url).
    Retorna lista de dicts dos posts efetivamente inseridos (com 'row_id').
    """
    now = datetime.now(timezone.utc).isoformat()
    inserted = []  # type: List[Dict[str, Any]]

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        for item in items:
            fields = _extract_post_fields(item)

            if fields is None:
                continue
            if not fields["username"] or not fields["url"]:
                continue
            if not fields["media_url"]:
                continue

            try:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO posts
                        (username, url, caption, likes, timestamp, scraped_at, media_url)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fields["username"],
                        fields["url"],
                        fields["caption"],
                        fields["likes"],
                        fields["timestamp"],
                        now,
                        fields["media_url"],
                    ),
                )
                if cur.rowcount == 1:
                    fields["row_id"] = cur.lastrowid
                    inserted.append(fields)
            except sqlite3.Error:
                continue
        conn.commit()

    return inserted


def _transcribe_new_posts(
    posts: List[Dict[str, Any]], db_path: str = DB_PATH
) -> int:
    """
    Transcreve os videos dos posts recem-inseridos e atualiza a coluna transcript.
    Retorna quantos foram transcritos com sucesso.
    """
    transcribed = 0
    with sqlite3.connect(db_path) as conn:
        for post in posts:
            row_id = post["row_id"]
            media_url = post["media_url"]
            username = post["username"]
            logger.info("TRANSCRIBE_START: @%s row_id=%d", username, row_id)

            text = transcribe_video(media_url)
            if text:
                conn.execute(
                    "UPDATE posts SET transcript = ? WHERE id = ?",
                    (text, row_id),
                )
                post["transcript"] = text
                transcribed += 1
                logger.info(
                    "TRANSCRIBE_OK: @%s %d chars", username, len(text)
                )
            else:
                logger.warning("TRANSCRIBE_FAIL: @%s", username)
        conn.commit()
    return transcribed


def _classify_new_posts(
    posts: List[Dict[str, Any]], db_path: str = DB_PATH
) -> List[Dict[str, Any]]:
    """
    Classifica posts que possuem transcript ou caption e ainda nao foram analisados.

    Atualiza as colunas analysis_json, analyzed_at e classifier_version no banco.
    Retorna lista de analises (dicts do classifier) para envio ao Telegram.
    """
    batch_analyses = []  # type: List[Dict[str, Any]]

    with sqlite3.connect(db_path) as conn:
        for post in posts:
            row_id = post["row_id"]
            username = post.get("username", "?")
            url = post.get("url", "")

            # Verifica se ja foi analisado
            cur = conn.execute(
                "SELECT analysis_json FROM posts WHERE id = ?", (row_id,)
            )
            row = cur.fetchone()
            if row and row[0]:
                logger.info(
                    "CLASSIFY_SKIP: @%s row_id=%d (ja analisado)", username, row_id
                )
                continue

            # Usa transcript (preferencia) ou caption
            text = post.get("transcript") or ""
            if not text:
                cur2 = conn.execute(
                    "SELECT transcript, caption FROM posts WHERE id = ?", (row_id,)
                )
                db_row = cur2.fetchone()
                if db_row:
                    text = db_row[0] or db_row[1] or ""

            if not text.strip():
                logger.info(
                    "CLASSIFY_SKIP: @%s row_id=%d (sem texto)", username, row_id
                )
                continue

            logger.info("CLASSIFY_START: @%s row_id=%d", username, row_id)

            try:
                analysis = classify_content(username, text, url)
            except Exception as e:
                logger.error(
                    "CLASSIFY_ERROR: @%s row_id=%d error=%s", username, row_id, e
                )
                continue

            if analysis is None:
                logger.warning(
                    "CLASSIFY_NONE: @%s row_id=%d (retornou None)", username, row_id
                )
                continue

            # Salva no banco
            analysis_str = json.dumps(analysis, ensure_ascii=False)
            analyzed_at = analysis.get(
                "analyzed_at",
                datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            version = analysis.get("classifier_version", "")

            conn.execute(
                """
                UPDATE posts
                SET analysis_json = ?, analyzed_at = ?, classifier_version = ?
                WHERE id = ?
                """,
                (analysis_str, analyzed_at, version, row_id),
            )

            batch_analyses.append(analysis)
            logger.info(
                "CLASSIFY_SAVED: @%s row_id=%d type=%s confidence=%.2f",
                username,
                row_id,
                analysis.get("content_type", "?"),
                analysis.get("confidence_score", 0),
            )

        conn.commit()

    return batch_analyses


def _send_telegram_message(text: str) -> None:
    """Envia mensagem simples ao Telegram (notificacao de status)."""
    import requests as req

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.error("TELEGRAM_MISSING_CREDENTIALS")
        return

    url = "https://api.telegram.org/bot{}/sendMessage".format(token)
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}

    try:
        response = req.post(url, json=payload, timeout=30)
        response.raise_for_status()
        logger.info("TELEGRAM_STATUS_SENT")
    except Exception as e:
        logger.error("TELEGRAM_STATUS_FAILED: %s", e)


def _run_apify_scraper(usernames: List[str]) -> List[Dict[str, Any]]:
    """Executa o scraper do Apify para coletar Reels dos perfis."""
    api_key = os.getenv("APIFY_API_KEY")
    if not api_key:
        raise RuntimeError("APIFY_API_KEY nao definido no .env.")

    client = ApifyClient(api_key)

    run_input = {
        "username": usernames,
        "resultsLimit": 2,
        "resultsType": "videos",
    }

    logger.info(
        "APIFY_START: %d perfis (apenas Reels)", len(usernames)
    )
    run = client.actor("apify/instagram-post-scraper").call(run_input=run_input)

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        logger.warning("APIFY_EMPTY: nenhum dado retornado")
        return []

    items = list(client.dataset(dataset_id).iterate_items())
    logger.info("APIFY_DONE: %d itens recebidos", len(items))
    return items


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    """Pipeline completo: scrape -> save -> transcribe -> classify -> report."""
    _load_env_file(".env")
    usernames = _read_influencers(INFLUENCERS_PATH)
    _init_db(DB_PATH)

    # 1. Coleta via Apify
    items = _run_apify_scraper(usernames)

    # 2. Filtro defensivo (max 2 por perfil)
    per_user = {}  # type: Dict[str, int]
    filtered = []  # type: List[Dict[str, Any]]
    for item in items:
        fields = _extract_post_fields(item)
        if fields is None:
            continue
        u = fields.get("username") or ""
        if not u:
            continue
        c = per_user.get(u, 0)
        if c >= 2:
            continue
        per_user[u] = c + 1
        filtered.append(item)

    # 3. Salva no banco
    new_posts = _save_posts(filtered, DB_PATH)
    logger.info("POSTS_SAVED: %d novos Reels", len(new_posts))

    # 4. Transcreve videos
    if new_posts:
        transcribed = _transcribe_new_posts(new_posts, DB_PATH)
        logger.info("TRANSCRIBE_DONE: %d/%d transcritos", transcribed, len(new_posts))

    # 5. Classifica conteudo politico
    batch_analyses = []  # type: List[Dict[str, Any]]
    if new_posts:
        batch_analyses = _classify_new_posts(new_posts, DB_PATH)
        logger.info("CLASSIFY_DONE: %d analises concluidas", len(batch_analyses))

    # 6. Envia relatorio ao Telegram (se houver conteudo relevante)
    if batch_analyses:
        logger.info("REPORT_START: enviando %d analises ao Telegram", len(batch_analyses))
        try:
            send_analysis_report(batch_analyses)
        except Exception as e:
            logger.error("REPORT_ERROR: %s", e)

    # 7. Notificacao simples de status
    _send_telegram_message(
        "Monitoramento concluido! {} Reels salvos, {} classificados.".format(
            len(new_posts), len(batch_analyses)
        )
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        logger.error("FATAL: %s", e)
        time.sleep(0.1)
        raise
