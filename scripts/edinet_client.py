# -*- coding: utf-8 -*-
"""
EDINET APIクライアント
- 書類一覧取得 (list_documents)
- XBRLインスタンスファイル取得 (fetch_xbrl_instance)
- APIレートリミット制御
"""
import os
import json
import requests
import time
import io
import zipfile
from threading import Lock
from typing import Optional, List

import config

# --- Module-level globals for session and rate limiting ---
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "my-edinet-screener/1.0"})

_RATE_LOCK = Lock()
_LAST_REQ_AT = 0.0

# --- Initialization ---
os.makedirs(config.DOC_CACHE_DIR, exist_ok=True)

# --- Private Functions ---
def _rate_limit():
    """APIアクセス間隔を制御する"""
    global _LAST_REQ_AT
    with _RATE_LOCK:
        now = time.time()
        delta = now - _LAST_REQ_AT
        if delta < config.MIN_INTERVAL_SEC:
            time.sleep(config.MIN_INTERVAL_SEC - delta)
        _LAST_REQ_AT = time.time()

# --- Public API Functions ---
def list_documents(target_date: str, api_key: str) -> List[dict]:
    """
    日別の提出一覧をキャッシュしつつ取得。戻り値は results(list[dict])。
    """
    cache_path = os.path.join(config.DOC_CACHE_DIR, f"{target_date}.json")
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
                # print(f"  -> Cache hit for {target_date}")
                return data.get("results") or []
        except Exception:
            pass  # 壊れたキャッシュは捨てて取り直す

    url = f"{config.EDINET_BASE_URL}/documents.json"
    params = {"date": target_date, "type": 2, "Subscription-Key": api_key}
    try:
        _rate_limit()
        r = SESSION.get(url, params=params, timeout=60)
        r.raise_for_status()
        response_json = r.json() or {}
        
        # キャッシュ保存
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(response_json, f, ensure_ascii=False)
        except Exception as e:
            print(f"  -> Cache write failed for {target_date}: {e}")
            
        return response_json.get("results") or []
    except requests.exceptions.RequestException as e:
        print(f"  -> HTTP Error list_documents({target_date}): {e}")
        return []
    except Exception as e:
        print(f"  -> Unexpected error list_documents({target_date}): {e}")
        return []

def fetch_xbrl_instance(doc_id: str, api_key: str) -> Optional[bytes]:
    """
    docIDのZIPを取得し、zipの中から.xbrl 本体(bytes)をメモリ上に返す
    """
    url = f"{config.EDINET_BASE_URL}/documents/{doc_id}"
    params = {"type": 1, "Subscription-Key": api_key}
    try:
        _rate_limit()
        with SESSION.get(url, params=params, stream=True, timeout=120) as r:
            r.raise_for_status()
            
            # コンテンツタイプをチェック
            content_type = r.headers.get("Content-Type", "")
            if "application/zip" not in content_type and "application/octet-stream" not in content_type:
                if "application/json" in content_type:
                    try:
                        error_details = r.json()
                        print(f"  -> API Error for docID {doc_id}: {error_details}")
                    except json.JSONDecodeError:
                        print(f"  -> API Error for docID {doc_id}: {r.text}")
                else:
                    print(f"  -> Unexpected Content-Type for docID {doc_id}: {content_type}")
                return None

            # メモリ上でzip展開
            zip_buffer = io.BytesIO(r.content)
            with zipfile.ZipFile(zip_buffer) as z:
                xbrl_files = [n for n in z.namelist() if n.lower().endswith(".xbrl")]
                if not xbrl_files:
                    print(f"  -> No .xbrl files in zip for docID {doc_id}")
                    return None
                
                # 「公開本体(publicdoc)」が含まれるファイルを優先
                xbrl_files.sort(key=lambda p: ("publicdoc" in p.lower(), "/xbrl/" in p.lower()), reverse=True)
                selected_xbrl_file = xbrl_files[0]
                
                with z.open(selected_xbrl_file) as f:
                    return f.read()

    except requests.exceptions.RequestException as e:
        print(f"  -> HTTP Error fetching docID {doc_id}: {e}")
        return None
    except zipfile.BadZipFile:
        print(f"  -> Bad zip file for docID {doc_id}")
        return None
    except Exception as e:
        print(f"  -> Unexpected error in fetch_xbrl_instance({doc_id}): {e}")
        return None
