import os
import json
from apify_client import ApifyClient

def _load_env_file(path: str = ".env") -> None:
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

_load_env_file(".env")

api_key = os.getenv("APIFY_API_KEY")
client = ApifyClient(api_key)

# Testa com UM perfil só
run_input = {
    "username": ["renansantosmbl"],
    "resultsLimit": 1,
    "resultsType": "videos"
}

print("🔍 Rodando Apify com resultsType='videos'...")
run = client.actor("apify/instagram-post-scraper").call(run_input=run_input)

dataset_id = run.get("defaultDatasetId")
items = list(client.dataset(dataset_id).iterate_items())

print(f"\n📊 Total de itens: {len(items)}")
print(f"\n🔎 Primeiro item completo (JSON):\n")
if items:
    print(json.dumps(items[0], indent=2, ensure_ascii=False))
else:
    print("❌ Nenhum item retornado!")