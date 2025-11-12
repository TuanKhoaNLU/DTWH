# main_job.py
import os
import json
from datetime import datetime
import config
from logging_setup import logger
import db
import apify_service

def job():
    """Pipeline công việc chính."""
    id_config = None # Khởi tạo
    try:
        logger.info("Job started for device %s", config.DEVICE_ID)

        # 1️⃣ Ghi cấu hình vào Config_log
        id_config = db.save_config_to_db()
        if not id_config:
            logger.warning("Could not get id_config, using default = 1 for logging")
            id_config = 1 # Sử dụng 1 giá trị tạm nếu không lưu được config

        # 2️⃣ Run Apify actor
        run_input = {
            "hashtags": ["fyp"],
            "resultsPerPage": 3,
        }

        run_id, dataset_id, items = apify_service.run_actor(
            config.APIFY_ACTOR, 
            config.APIFY_TOKEN, 
            run_input
        )
        logger.info("Actor run completed: %s (%d items)", run_id, len(items))

        # 3️⃣ Save dataset to file (Cho loader.py của đồng nghiệp)
        ts = datetime.now().strftime("%d%m%YT%H%M%SZ")
        fname = f"{config.DEVICE_ID}_run_{ts}.json"
        fpath = os.path.join(config.STORAGE_PATH, fname)
        
        with open(fpath, "w", encoding="utf-8") as fw:
            json.dump(items, fw, ensure_ascii=False)
        logger.info("Saved data to file: %s", fpath)

        # 4️⃣ (save_to_staging() ĐÃ BỊ XÓA - đây là việc của loader.py)

        # 5️⃣ Ghi log thành công
        db.log_to_db("SUCCESS", fname, total_record=len(items), id_config=id_config)
        logger.info("Job finished successfully for device %s", config.DEVICE_ID)

    except Exception as e:
        logger.exception("Job failed: %s", e)
        # Ghi log thất bại
        db.log_to_db("FAILED", "Apify Crawl", total_record=0, error_message=str(e), id_config=id_config)