# app.py
import os
import time
import json
import yaml
import logging
import pymysql
import requests
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler
from dotenv import load_dotenv
from apify_client import ApifyClient

# Load env & config
load_dotenv()
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config.yml")
with open(CONFIG_PATH, "r") as f:
    cfg = yaml.safe_load(f)

APIFY_TOKEN = os.getenv("APIFY_TOKEN") or cfg.get("apify", {}).get("token")
APIFY_ACTOR = os.getenv("APIFY_ACTOR") or cfg.get("apify", {}).get("actor_id")
APIFY_ENABLED = (cfg.get("apify", {}).get("enabled", True) is not False)
DEVICE_ID = cfg.get("app", {}).get("device_id", "device-unknown")
STORAGE_PATH = cfg.get("app", {}).get("storage_path", "/data/storage")

MYSQL_CONF = cfg.get("mysql") or {}
MYSQL_HOST = os.getenv("MYSQL_HOST", MYSQL_CONF.get("host", "db"))
MYSQL_PORT = int(os.getenv("MYSQL_PORT", MYSQL_CONF.get("port", 3306)))
MYSQL_USER = os.getenv("MYSQL_USER", MYSQL_CONF.get("user"))
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", MYSQL_CONF.get("password"))
MYSQL_DB = os.getenv("MYSQL_DATABASE", MYSQL_CONF.get("database"))

os.makedirs(STORAGE_PATH, exist_ok=True)

# ---- Database helper ----
def get_db_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )

# ---- Logger setup ----
logger = logging.getLogger("crawler")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(ch)

# ---- Save config to DB ----
def save_config_to_db():
    """
    Đọc thông tin config crawl từ config.yml và lưu vào bảng Config_log nếu chưa có.
    Trả về id_config (đã tồn tại hoặc vừa tạo).
    """
    conn = get_db_conn()
    try:
        source_name = cfg.get("source", {}).get("name", "Apify Crawl")
        source_url = cfg.get("source", {}).get("url", "")
        api_endpoint = cfg.get("apify", {}).get("actor_id", "")
        file_path = STORAGE_PATH
        file_pattern = cfg.get("app", {}).get("file_pattern", "*.json")
        date_format = cfg.get("app", {}).get("date_format", "%Y-%m-%d")
        schedule_time = cfg.get("schedule", {}).get("cron", "")
        is_active = True

        with conn.cursor() as cur:
            # Kiểm tra tồn tại
            check_sql = "SELECT id_config FROM Config_log WHERE source_name=%s AND source_url=%s"
            cur.execute(check_sql, (source_name, source_url))
            row = cur.fetchone()
            if row:
                return row["id_config"]

            # Thêm mới
            insert_sql = """
                INSERT INTO Config_log (source_name, source_url, api_endpoint, file_path, file_pattern, date_format, schedule_time, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(insert_sql, (
                source_name, source_url, api_endpoint, file_path, file_pattern,
                date_format, schedule_time, is_active
            ))
            return cur.lastrowid
    except Exception as e:
        logger.error("Failed to save Config_log: %s", e)
        return None
    finally:
        conn.close()

# ---- log_to_db() ----
def log_to_db(status, message, total_record=0, error_message=None, id_config=None):
    """Ghi log crawl vào bảng Control_log."""
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            sql = """
                INSERT INTO Control_log (id_config, file_name, status, extract_time, total_record, error_message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql, (
                id_config,
                message,  # file_name hoặc mô tả
                status,
                datetime.now(timezone.utc),
                total_record,
                error_message
            ))
    except Exception as e:
        logger.error("Failed to write Control_log: %s", e)
    finally:
        if 'conn' in locals():
            conn.close()

# ---- save_to_staging() ----
def save_to_staging(run_id, items, file_path):
    """Lưu dữ liệu crawl được vào bảng staging_raw."""
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            sql = """
                INSERT INTO staging_raw (device_id, apify_run_id, raw_json, file_path)
                VALUES (%s, %s, %s, %s)
            """
            for item in items:
                cur.execute(sql, (DEVICE_ID, run_id, json.dumps(item, ensure_ascii=False), file_path))
        logger.info("Inserted %d items into staging_raw", len(items))
    except Exception as e:
        logger.exception("Error inserting into staging_raw: %s", e)
    finally:
        if 'conn' in locals():
            conn.close()

# ---- Apify interaction ----
def run_actor(actor_id, apify_token, run_input):
    """Gọi Apify actor và trả về (run_id, dataset_id, results)."""
    client = ApifyClient(apify_token)
    run = client.actor(actor_id).call(run_input=run_input)
    dataset_id = run.get("defaultDatasetId")
    run_id = run.get("id")
    results = [item for item in client.dataset(dataset_id).iterate_items()]
    return run_id, dataset_id, results

# ---- Fallback: load local JSON when Apify disabled or fails ----
def load_local_latest():
    try:
        files = [f for f in os.listdir(STORAGE_PATH) if f.lower().endswith('.json')]
        if not files:
            return None, []
        files.sort(reverse=True)
        latest = files[0]
        fpath = os.path.join(STORAGE_PATH, latest)
        with open(fpath, 'r', encoding='utf-8') as fr:
            try:
                data = json.load(fr)
            except json.JSONDecodeError:
                fr.seek(0)
                data = [json.loads(line) for line in fr if line.strip()]
        return f"local-{latest}", (data if isinstance(data, list) else [data])
    except Exception as ex:
        logger.error("Failed to load local JSON: %s", ex)
        return None, []

# ---- Main job ----
def job():
    try:
        logger.info("Job started for device %s", DEVICE_ID)

        # 1️⃣ Ghi cấu hình vào Config_log
        id_config = save_config_to_db()
        if not id_config:
            logger.warning("Could not get id_config, using default = 1")
            id_config = 1

        # 2️⃣ Run Apify actor (or fallback to local files)
        run_input = {
            "hashtags": ["fyp"],
            "resultsPerPage": 100,
            "profiles": [],
            "profileScrapeSections": ["videos"],
            "excludePinnedPosts": False,
            "maxProfilesPerQuery": 10,
            "scrapeRelatedVideos": False,
            "shouldDownloadVideos": False
        }
        items = []
        run_id = None
        if APIFY_ENABLED and APIFY_TOKEN and APIFY_ACTOR:
            try:
                run_id, dataset_id, items = run_actor(APIFY_ACTOR, APIFY_TOKEN, run_input)
                logger.info("Actor run completed: %s (%d items)", run_id, len(items))
            except Exception as apify_err:
                logger.warning("Apify failed (%s). Falling back to local files.", apify_err)
                run_id, items = load_local_latest()
        else:
            logger.info("Apify disabled or not configured. Loading local files.")
            run_id, items = load_local_latest()
        if not items:
            raise RuntimeError("No data available from Apify or local storage")

        # 3️⃣ Save dataset to file
        ts = datetime.now(timezone.utc).strftime("%d%m%YT%H%M%SZ")
        fname = f"{DEVICE_ID}_run_{ts}.json"
        fpath = os.path.join(STORAGE_PATH, fname)
        with open(fpath, "w", encoding="utf-8") as fw:
            json.dump(items, fw, ensure_ascii=False)

        # 4️⃣ Save to staging_raw
        save_to_staging(run_id, items, fpath)

        # 5️⃣ Log success
        log_to_db("SUCCESS", fname, total_record=len(items), id_config=id_config)
        logger.info("Job finished successfully for device %s", DEVICE_ID)

    except Exception as e:
        logger.exception("Job failed: %s", e)
        log_to_db("FAILED", "Apify Crawl", total_record=0, error_message=str(e))

# ---- Scheduler ----
def start_scheduler():
    sched = BlockingScheduler(timezone=cfg.get("schedule", {}).get("timezone", "UTC"))
    cron_expr = cfg.get("schedule", {}).get("cron")

    if cfg.get("schedule", {}).get("enabled", True) and cron_expr:
        parts = cron_expr.split()
        if len(parts) == 5:
            minute, hour, day, month, day_of_week = parts
            sched.add_job(job, 'cron', minute=minute, hour=hour, day=day,
                          month=month, day_of_week=day_of_week, id="apify_job")
            logger.info("Scheduled job with cron: %s", cron_expr)
        else:
            logger.warning("Invalid cron expression, running once immediately.")
            job()
    else:
        logger.info("Scheduler disabled; running once.")
        job()
        return

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")

if __name__ == "__main__":
    start_scheduler()
