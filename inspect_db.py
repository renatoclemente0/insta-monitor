import sqlite3

DB_PATH = "monitor.db"

def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # mostra colunas existentes
    cur.execute("PRAGMA table_info(posts)")
    cols = [row[1] for row in cur.fetchall()]
    print("Columns in posts:", cols)

    # puxa 5 linhas (username, url, media_url)
    cur.execute("SELECT username, url, media_url FROM posts LIMIT 5")
    rows = cur.fetchall()

    print("\nSample rows:")
    for r in rows:
        print(r)

    conn.close()

if __name__ == "__main__":
    main()