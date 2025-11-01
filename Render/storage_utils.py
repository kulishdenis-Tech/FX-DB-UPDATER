# storage_utils.py
import os
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
BUCKET_NAME = "raw"

def get_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def upload_text(name: str, content: str, upsert: bool = True) -> None:
    """
    Завантажує файл до Supabase bucket 'raw'.
    Якщо файл існує — перезаписує (x-upsert: true).
    """
    sb = get_client()
    try:
        headers = {
            "content-type": "text/plain",
            "x-upsert": "true" if upsert else "false",
        }
        sb.storage.from_(BUCKET_NAME).upload(name, content.encode("utf-8"), headers)
        print(f"[SUPABASE] ✅ Uploaded: {name}")
    except Exception as e:
        print(f"[SUPABASE] ❌ Upload error for {name}: {e}")


def download_text(name: str) -> str:
    """
    Завантажує файл з bucket 'raw' і повертає вміст як UTF-8 рядок.
    Якщо файл не існує — повертає порожній рядок.
    """
    sb = get_client()
    try:
        data = sb.storage.from_(BUCKET_NAME).download(name)
        if not data:
            print(f"[SUPABASE] ⚠️ File not found: {name}")
            return ""
        return data.decode("utf-8", errors="ignore")
    except Exception as e:
        print(f"[SUPABASE] ⚠️ Download error for {name}: {e}")
        return ""