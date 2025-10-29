CREATE DATABASE IF NOT EXISTS dwh_tiktok;
USE dwh_tiktok;

-- Lưu trữ dữ liệu thô lấy từ Apify
CREATE TABLE IF NOT EXISTS staging_raw (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    device_id VARCHAR(128) NOT NULL,
    fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    apify_run_id VARCHAR(255) NULL,
    raw_json JSON NOT NULL,
    file_path VARCHAR(1024) NULL,
    processed BOOLEAN DEFAULT FALSE,
    UNIQUE KEY (apify_run_id, id)
);

-- Cấu hình nguồn dữ liệu crawl
CREATE TABLE IF NOT EXISTS Config_log (
    id_config INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(255),
    source_url VARCHAR(1024),
    api_endpoint VARCHAR(1024),
    file_path TEXT,
    file_pattern VARCHAR(255),
    date_format VARCHAR(64),
    schedule_time VARCHAR(64),
    is_active BOOLEAN DEFAULT TRUE
);

-- Ghi lại trạng thái từng lần crawl
CREATE TABLE IF NOT EXISTS Control_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_config INT,
    file_name VARCHAR(255),
    status VARCHAR(64),
    extract_time DATETIME,
    total_record INT,
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (id_config) REFERENCES Config_log(id_config)
);

CREATE TABLE IF NOT EXISTS dim_authors (
    authorID INT AUTO_INCREMENT PRIMARY KEY,
    authorName VARCHAR(255),
    avatarUrl TEXT,
    authorCategory VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_videos (
    videoID BIGINT PRIMARY KEY,
    authorID INT,
    textContent TEXT,
    duration INT,
    createTime DATETIME,
    webVideoUrl TEXT,
    hashtagList TEXT,
    FOREIGN KEY (authorID) REFERENCES dim_authors(authorID)
);

CREATE TABLE IF NOT EXISTS dim_date (
    dateKey INT PRIMARY KEY,
    day VARCHAR(16),
    date DATE
);

CREATE TABLE IF NOT EXISTS fact_videos (
    interactionID INT AUTO_INCREMENT PRIMARY KEY,
    videoID BIGINT,
    authorID INT,
    dateKey INT,
    timeLogKey INT,
    diggCount INT,
    shareCount INT,
    playCount INT,
    commentCount INT,
    collectCount INT,
    createdAt DATETIME,
    FOREIGN KEY (videoID) REFERENCES dim_videos(videoID),
    FOREIGN KEY (authorID) REFERENCES dim_authors(authorID),
    FOREIGN KEY (dateKey) REFERENCES dim_date(dateKey)
);

CREATE INDEX idx_fact_videos_date ON fact_videos(dateKey);
CREATE INDEX idx_fact_videos_author ON fact_videos(authorID);
CREATE INDEX idx_dim_videos_author ON dim_videos(authorID);
