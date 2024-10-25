import json
import config
import requests


def search_vectors():
    BASE_URL = "https://data.dev.agione.ai/api/v1/data"

    headers = {
        'api-key': config.db_api_key,
        'Content-Type': 'application/json'
    }
    url = f"{BASE_URL}/vectors/"
    params = {
        "model": "text-embedding-3-small", # 可选, 默认是text-embedding-3-small 目前只适配 text-embedding-3-small, text-embedding-ada-002, text-embedding-3-large
        "dimension": 1536, # 可选, 默认1536
        "type": "Knowledge Collect - Project",
        "metadata": json.dumps({}),
        "skip": 0, # 可选, 默认0
        "limit": 1000000000000  # 可选, 默认10
    }
    response = requests.get(url, params=params, headers=headers,timeout=10)
    print(f"Search Vectors Response: {response.status_code}")
    if response.status_code==200:
        return response.json()
    else:
        return []