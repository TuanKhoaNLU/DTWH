import os
import json
import glob
from datetime import datetime, timezone

import pymysql
import yaml
from dotenv import load_dotenv


load_dotenv()

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config.yml")
if os.path.exists(CONFIG_PATH):
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
else:
    cfg = {}

MYSQL_HOST = os.getenv("MYSQL_HOST", (cfg.get("mysql") or {}).get("host", "db"))
MYSQL_PORT = int(os.getenv("MYSQL_PORT", (cfg.get("mysql") or {}).get("port", 3306)))
MYSQL_USER = os.getenv("MYSQL_USER", (cfg.get("mysql") or {}).get("user"))
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", (cfg.get("mysql") or {}).get("password"))

# Always write into dbStaging by default, can be overridden via env
MYSQL_DB = os.getenv("MYSQL_DATABASE_STAGING", "dbStaging")

STORAGE_PATH = (cfg.get("app") or {}).get("storage_path", "/data/storage")


def get_db_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        db=MYSQL_DB,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
    )


def ensure_schema():
    # The init_db script should have created the schema; this is a safety net.
    ddl = [
        "CREATE DATABASE IF NOT EXISTS dbStaging",
        "CREATE TABLE IF NOT EXISTS dbStaging.Authors (authorID BIGINT PRIMARY KEY, Name VARCHAR(255), avatar TEXT)",
        """
        CREATE TABLE IF NOT EXISTS dbStaging.Videos (
            videoID BIGINT PRIMARY KEY,
            authorID BIGINT,
            TextContent TEXT,
            Duration INT,
            CreateTime DATETIME,
            WebVideoUrl TEXT,
            FOREIGN KEY (authorID) REFERENCES dbStaging.Authors(authorID)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS dbStaging.VideoInteractions (
            interactionID BIGINT AUTO_INCREMENT PRIMARY KEY,
            videoID BIGINT UNIQUE,
            DiggCount INT,
            PlayCount BIGINT,
            ShareCount INT,
            CommentCount INT,
            CollectCount INT,
            FOREIGN KEY (videoID) REFERENCES dbStaging.Videos(videoID)
        )
        """,
    ]
    root_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with root_conn.cursor() as cur:
            for stmt in ddl:
                cur.execute(stmt)
    finally:
        root_conn.close()


def _g(obj, *keys, default=None):
    cur = obj
    for k in keys:
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return default
    return cur


def _parse_int(value, default=None):
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _parse_epoch_to_dt(value):
    if value is None:
        return None
    try:
        # Some sources provide string epoch
        ts = int(value)
        return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        try:
            # Try ISO string
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None


def upsert_author(cur, author_id, name, avatar):
    sql = (
        "INSERT INTO Authors (authorID, Name, avatar) VALUES (%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE Name=VALUES(Name), avatar=VALUES(avatar)"
    )
    cur.execute(sql, (author_id, name, avatar))


def upsert_video(cur, video_id, author_id, text, duration, create_time, url):
    sql = (
        "INSERT INTO Videos (videoID, authorID, TextContent, Duration, CreateTime, WebVideoUrl) "
        "VALUES (%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE authorID=VALUES(authorID), TextContent=VALUES(TextContent), Duration=VALUES(Duration), "
        "CreateTime=VALUES(CreateTime), WebVideoUrl=VALUES(WebVideoUrl)"
    )
    cur.execute(sql, (video_id, author_id, text, duration, create_time, url))


def upsert_interactions(cur, video_id, digg, play, share, comment, collect):
    sql = (
        "INSERT INTO VideoInteractions (videoID, DiggCount, PlayCount, ShareCount, CommentCount, CollectCount) "
        "VALUES (%s,%s,%s,%s,%s,%s) "
        "ON DUPLICATE KEY UPDATE DiggCount=VALUES(DiggCount), PlayCount=VALUES(PlayCount), ShareCount=VALUES(ShareCount), "
        "CommentCount=VALUES(CommentCount), CollectCount=VALUES(CollectCount)"
    )
    cur.execute(sql, (video_id, digg, play, share, comment, collect))


def extract_record(item):
    # Video ID
    video_id = (
        item.get("id")
        or _g(item, "video", "id")
        or _g(item, "itemInfo", "itemId")
    )

    # Author fields
    author_meta = (
        item.get("authorMeta")
        or item.get("author")
        or _g(item, "author", default={})
        or {}
    )
    author_id = (
        author_meta.get("id")
        or _g(item, "author", "id")
        or _g(item, "author", "secUid")
        or _g(item, "author", "uniqueId")
    )
    author_name = (
        author_meta.get("name")
        or author_meta.get("uniqueId")
        or _g(item, "author", "uniqueId")
    )
    avatar = (
        author_meta.get("avatar")
        or _g(item, "author", "avatarThumb")
        or _g(item, "author", "avatarMedium")
        or _g(item, "author", "avatarLarger")
    )

    # Video fields
    text = item.get("text") or item.get("desc") or _g(item, "itemInfo", "text")
    duration = _parse_int(item.get("duration") or _g(item, "video", "duration"))
    create_time = _parse_epoch_to_dt(item.get("createTime") or _g(item, "itemInfo", "createTime"))
    web_url = item.get("webVideoUrl") or item.get("shareUrl") or item.get("url")

    # Interaction fields (robust across actor shapes)
    stats = item.get("stats") or item.get("statistics") or {}
    digg = _parse_int(item.get("diggCount") or stats.get("diggCount") or stats.get("likeCount"), 0)
    play = _parse_int(item.get("playCount") or stats.get("playCount") or stats.get("playCountSum"), 0)
    share = _parse_int(item.get("shareCount") or stats.get("shareCount"), 0)
    comment = _parse_int(item.get("commentCount") or stats.get("commentCount"), 0)
    collect = _parse_int(item.get("collectCount") or stats.get("collectCount") or stats.get("saveCount"), 0)

    return {
        "video_id": str(video_id) if video_id is not None else None,
        "author_id": str(author_id) if author_id is not None else None,
        "author_name": author_name,
        "avatar": avatar,
        "text": text,
        "duration": duration,
        "create_time": create_time,
        "web_url": web_url,
        "digg": digg,
        "play": play,
        "share": share,
        "comment": comment,
        "collect": collect,
    }


def load_file_to_db(path):
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            # fallback for JSON lines
            f.seek(0)
            data = [json.loads(line) for line in f if line.strip()]

    if not isinstance(data, list):
        data = [data]

    conn = get_db_conn()
    inserted = 0
    try:
        with conn.cursor() as cur:
            for item in data:
                rec = extract_record(item)
                if not rec["video_id"]:
                    continue
                # Author may be missing; allow NULL authorID in Videos
                if rec["author_id"]:
                    upsert_author(cur, rec["author_id"], rec["author_name"], rec["avatar"])
                upsert_video(cur, rec["video_id"], rec["author_id"], rec["text"], rec["duration"], rec["create_time"], rec["web_url"])
                upsert_interactions(cur, rec["video_id"], rec["digg"], rec["play"], rec["share"], rec["comment"], rec["collect"])
                inserted += 1
    finally:
        conn.close()
    return inserted


def main():
    ensure_schema()
    files = sorted(glob.glob(os.path.join(STORAGE_PATH, "*.json")))
    if not files:
        print("No JSON files found in", STORAGE_PATH)
        return
    total = 0
    for fp in files:
        try:
            count = load_file_to_db(fp)
            print(f"Loaded {count} items from {os.path.basename(fp)}")
            total += count
        except Exception as e:
            print(f"Failed to load {fp}: {e}")
    print(f"Done. Total rows processed: {total}")


if __name__ == "__main__":
    main()


