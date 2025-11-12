# db.py
import pymysql
from datetime import datetime
import config  # Vẫn import config để lấy thông tin host, user, pass
from logging_setup import logger

def get_db_conn():
    """
    Returns a new database connection.
    LƯU Ý: Đã sửa 'db' để trỏ thẳng vào 'metadata_tiktok'
    """
    return pymysql.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
        db="metadata_tiktok",  # SỬA Ở ĐÂY: Trỏ đúng vào DB chứa log
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor
    )

def save_config_to_db():
    """
    Đọc thông tin config crawl và lưu vào bảng config_log (chữ thường)
    """
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            # SỬA Ở ĐÂY: dùng 'config_log' (chữ thường)
            check_sql = "SELECT id_config FROM config_log WHERE source_name=%s AND source_url=%s"
            cur.execute(check_sql, (config.SOURCE_NAME, config.SOURCE_URL))
            row = cur.fetchone()
            if row:
                return row["id_config"]

            # SỬA Ở ĐÂY: dùng 'config_log' (chữ thường)
            insert_sql = """
                INSERT INTO config_log (source_name, source_url, api_endpoint, file_path, file_pattern, date_format, schedule_time, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.execute(insert_sql, (
                config.SOURCE_NAME, config.SOURCE_URL, config.APIFY_ACTOR, 
                config.STORAGE_PATH, config.FILE_PATTERN, config.DATE_FORMAT, 
                config.SCHEDULE_CRON, True
            ))
            return cur.lastrowid
    except Exception as e:
        logger.error("Failed to save config_log: %s", e)
        return None
    finally:
        conn.close()

def log_to_db(status, message, total_record=0, error_message=None, id_config=None):
    """Ghi log crawl vào bảng control_log (chữ thường)"""
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            # SỬA Ở ĐÂY: dùng 'control_log' (chữ thường)
            sql = """
                INSERT INTO control_log (id_config, file_name, status, extract_time, total_record, error_message)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.execute(sql, (
                id_config,
                message,
                status,
                datetime.now(),
                total_record,
                error_message
            ))
    except Exception as e:
        logger.error("Failed to write control_log: %s", e)
    finally:
        if 'conn' in locals():
            conn.close()