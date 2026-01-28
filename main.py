import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
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
            # Não sobrescreve variáveis já definidas no ambiente
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
            # Normaliza @ se existir
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
        conn.commit()


def _to_iso_utc(value) -> str | None:
    """
    Converte diferentes formatos comuns em ISO-8601 UTC.
    Aceita:
    - epoch em segundos/milisegundos
    - string ISO
    - None
    """
    if value is None:
        return None

    # epoch numérico
    if isinstance(value, (int, float)):
        v = float(value)
        # heurística para ms
        if v > 10_000_000_000:
            v = v / 1000.0
        dt = datetime.fromtimestamp(v, tz=timezone.utc)
        return dt.isoformat()

    # string
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # tenta epoch em string
        if s.isdigit():
            return _to_iso_utc(int(s))
        # tenta ISO
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return s

    return str(value)


def _extract_post_fields(item: dict) -> dict:
    """
    Tenta extrair campos do output do actor (estrutura pode variar).
    """
    username = (
        item.get("ownerUsername")
        or item.get("username")
        or (item.get("owner", {}) or {}).get("username")
        or (item.get("author", {}) or {}).get("username")
    )
    url = item.get("url") or item.get("postUrl") or item.get("permalink")
    caption = item.get("caption") or item.get("text") or item.get("description")

    likes = (
        item.get("likesCount")
        or item.get("likes")
        or item.get("likeCount")
        or (item.get("edge_media_preview_like", {}) or {}).get("count")
    )
    try:
        likes = int(likes) if likes is not None else None
    except Exception:
        likes = None

    ts = (
        item.get("timestamp")
        or item.get("takenAtTimestamp")
        or item.get("takenAt")
        or item.get("createdAt")
        or item.get("date")
    )
    timestamp = _to_iso_utc(ts)

    return {
        "username": username,
        "url": url,
        "caption": caption,
        "likes": likes,
        "timestamp": timestamp,
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
            if not fields["username"] or not fields["url"]:
                continue
            try:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO posts (username, url, caption, likes, timestamp, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        fields["username"],
                        fields["url"],
                        fields["caption"],
                        fields["likes"],
                        fields["timestamp"],
                        now,
                    ),
                )
                if cur.rowcount == 1:
                    inserted += 1
            except sqlite3.Error:
                # não interrompe o monitoramento por um item malformado
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

    # ✅ Input ajustado para o que o robô do Apify exige
    run_input = {
        "username": usernames,
        "resultsLimit": 2,
        "resultsType": "posts"
    }

    print(f"⏳ Iniciando coleta no Apify para {len(usernames)} perfis...")
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

    # Se o actor retornar mais de 2 por perfil, fazemos um corte defensivo por username.
    per_user: dict[str, int] = {}
    filtered: list[dict] = []
    for item in items:
        fields = _extract_post_fields(item)
        u = fields.get("username") or ""
        if not u:
            continue
        c = per_user.get(u, 0)
        if c >= 2:
            continue
        per_user[u] = c + 1
        filtered.append(item)

    saved = _save_posts(filtered, DB_PATH)

    msg = f"✅ Monitoramento concluído! {saved} posts foram analisados."
    _send_telegram_message(msg)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        # garante feedback no console e falha no exit code, sem “engolir” erro
        print(f"Erro no monitoramento: {e}", file=sys.stderr)
        time.sleep(0.1)
        raise

