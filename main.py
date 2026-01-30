import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

from apify_client import ApifyClient


DB_PATH = "monitor.db"
INFLUENCERS_PATH = "influencers.txt"


def _load_env_file(path: str = ".env") -> None:
    """
    Carrega um arquivo .env simples (KEY=VALUE) para os.environ.
    Não depende de python-dotenv, para manter o projeto leve.
    """
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def _read_influencers(path: str = INFLUENCERS_PATH) -> list[str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo '{path}' não encontrado.")

    usernames: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            u = raw_line.strip()
            if not u or u.startswith("#"):
                continue
            if u.startswith("@"):
                u = u[1:]
            usernames.append(u)

    if not usernames:
        raise ValueError(f"Nenhum perfil válido encontrado em '{path}'.")

    return usernames


def _init_db(db_path: str = DB_PATH) -> None:
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

        conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_username ON posts(username)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_timestamp ON posts(timestamp)")

        def add_column_if_missing(column_name: str, column_type: str) -> None:
            cur = conn.execute("PRAGMA table_info(posts)")
            existing = {row[1] for row in cur.fetchall()}
            if column_name not in existing:
                conn.execute(f"ALTER TABLE posts ADD COLUMN {column_name} {column_type}")

        add_column_if_missing("media_url", "TEXT")
        add_column_if_missing("transcript", "TEXT")
        add_column_if_missing("ai_label", "TEXT")
        add_column_if_missing("ai_score", "INTEGER")
        add_column_if_missing("ai_summary", "TEXT")
        add_column_if_missing("ai_reason", "TEXT")
        add_column_if_missing("ai_ran_at", "TEXT")

        conn.commit()


def _to_iso_utc(value) -> str | None:
    """
    Converte diferentes formatos comuns em ISO-8601 UTC.
    """
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


def _extract_post_fields(item: dict) -> dict | None:
    """
    Extrai campos do post. Retorna None se não for vídeo (Reel).
    """
    # Verifica se é vídeo direto ou tem vídeo em childPosts
    is_video = item.get("type") == "Video"
    has_video_child = False
    
    if not is_video and isinstance(item.get("childPosts"), list):
        for child in item["childPosts"]:
            if child.get("type") == "Video":
                has_video_child = True
                break

    # ❌ Se não for vídeo, ignora esse post
    if not (is_video or has_video_child):
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

    # Extração da URL do vídeo
    media_url = None

    # Caso 1: post direto é vídeo
    if item.get("type") == "Video":
        media_url = item.get("videoUrl") or item.get("video_url") or item.get("displayUrl")

    # Caso 2: carrossel (Sidecar) com vídeos dentro
    if not media_url and isinstance(item.get("childPosts"), list):
        for child in item["childPosts"]:
            if child.get("type") == "Video":
                media_url = child.get("videoUrl") or child.get("video_url") or child.get("displayUrl")
                if media_url:
                    break

    return {
        "username": username,
        "url": url,
        "caption": caption,
        "likes": likes,
        "timestamp": timestamp,
        "media_url": media_url,
    }


def _save_posts(items: list[dict], db_path: str = DB_PATH) -> int:
    """
    Salva posts no SQLite, evitando duplicados por (username, url).
    Retorna quantos foram efetivamente inseridos.
    """
    now = datetime.now(timezone.utc).isoformat()
    inserted = 0

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        for item in items:
            fields = _extract_post_fields(item)
            
            # ❌ Se retornou None (não é vídeo), pula
            if fields is None:
                continue
                
            if not fields["username"] or not fields["url"]:
                continue

            # Ignora posts sem URL de vídeo válida
            if not fields["media_url"]:
                continue
                
            try:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO posts (username, url, caption, likes, timestamp, scraped_at, media_url)
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
                    inserted += 1
            except sqlite3.Error:
                continue
        conn.commit()

    return inserted


def _send_telegram_message(text: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN e/ou TELEGRAM_CHAT_ID não definidos no .env.")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    
    import requests
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    print("📱 Mensagem enviada para o Telegram!")


def _run_apify_scraper(usernames: list[str]) -> list[dict]:
    api_key = os.getenv("APIFY_API_KEY")
    if not api_key:
        raise RuntimeError("APIFY_API_KEY não definido no .env.")

    client = ApifyClient(api_key)

    # ✅ Input ajustado para pegar APENAS vídeos (Reels)
    run_input = {
        "username": usernames,
        "resultsLimit": 2,
        "resultsType": "videos"  # ← MUDANÇA AQUI
    }

    print(f"⏳ Iniciando coleta no Apify para {len(usernames)} perfis (apenas Reels)...")
    run = client.actor("apify/instagram-post-scraper").call(run_input=run_input)
    
    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        print("⚠️ Nenhum dado retornado pelo Apify.")
        return []

    items = list(client.dataset(dataset_id).iterate_items())
    print(f"✅ Coleta finalizada. {len(items)} itens recebidos.")
    return items


def main() -> int:
    _load_env_file(".env")
    usernames = _read_influencers(INFLUENCERS_PATH)
    _init_db(DB_PATH)

    items = _run_apify_scraper(usernames)

    # Filtro defensivo por username (máximo 2 por perfil)
    per_user: dict[str, int] = {}
    filtered: list[dict] = []
    for item in items:
        fields = _extract_post_fields(item)
        if fields is None:  # não é vídeo
            continue
        u = fields.get("username") or ""
        if not u:
            continue
        c = per_user.get(u, 0)
        if c >= 2:
            continue
        per_user[u] = c + 1
        filtered.append(item)

    saved = _save_posts(filtered, DB_PATH)

    msg = f"✅ Monitoramento concluído! {saved} Reels foram salvos."
    _send_telegram_message(msg)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"Erro no monitoramento: {e}", file=sys.stderr)
        time.sleep(0.1)
        raise