CREATE DATABASE IF NOT EXISTS metadata_tiktok;
CREATE DATABASE IF NOT EXISTS staging_tiktok;
CREATE DATABASE IF NOT EXISTS warehouse_tiktok;

-- metadata
USE metadata_tiktok;

CREATE TABLE IF NOT EXISTS config_log (
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

CREATE TABLE IF NOT EXISTS control_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    id_config INT,
    file_name VARCHAR(255),
    status VARCHAR(64),
    extract_time DATETIME,
    total_record INT,
    error_message TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    update_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (id_config) REFERENCES config_log(id_config)
);

-- staging
USE staging_tiktok;

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

-- warehouse
USE warehouse_tiktok;

CREATE TABLE IF NOT EXISTS dim_authors (
    authorID BIGINT PRIMARY KEY,
    authorName VARCHAR(255),
    avatarUrl TEXT,
    authorCategory VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_videos (
    videoID BIGINT PRIMARY KEY,
    authorID BIGINT,
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
    interactionID BIGINT AUTO_INCREMENT PRIMARY KEY,
    videoID BIGINT,
    authorID BIGINT,
    dateKey INT,
    diggCount BIGINT,
    shareCount BIGINT,
    playCount BIGINT,
    commentCount BIGINT,
    collectCount BIGINT,
    createdAt DATETIME,
    FOREIGN KEY (videoID) REFERENCES dim_videos(videoID),
    FOREIGN KEY (authorID) REFERENCES dim_authors(authorID),
    FOREIGN KEY (dateKey) REFERENCES dim_date(dateKey)
);

CREATE INDEX idx_fact_videos_date ON fact_videos(dateKey);
CREATE INDEX idx_fact_videos_author ON fact_videos(authorID);
CREATE INDEX idx_dim_videos_author ON dim_videos(authorID);

-- ------------------------------
-- Staging schema for raw TikTok entities (Authors, Videos, Interactions)
-- ------------------------------
CREATE DATABASE IF NOT EXISTS dbStaging;
-- Create tables according to the provided specification
CREATE TABLE IF NOT EXISTS dbStaging.Authors (
    authorID BIGINT PRIMARY KEY,
    Name VARCHAR(255),
    avatar TEXT
);

CREATE TABLE IF NOT EXISTS dbStaging.Videos (
    videoID BIGINT PRIMARY KEY,
    authorID BIGINT,
    TextContent TEXT,
    Duration INT,
    CreateTime DATETIME,
    WebVideoUrl TEXT,
    FOREIGN KEY (authorID) REFERENCES dbStaging.Authors(authorID)
);

CREATE TABLE IF NOT EXISTS dbStaging.VideoInteractions (
    interactionID BIGINT AUTO_INCREMENT PRIMARY KEY,
    videoID BIGINT UNIQUE,
    DiggCount INT,
    PlayCount BIGINT,
    ShareCount INT,
    CommentCount INT,
    CollectCount INT,
    FOREIGN KEY (videoID) REFERENCES dbStaging.Videos(videoID)
);

CREATE DATABASE IF NOT EXISTS dbAgg;
USE dbAgg;

-- Bảng tổng hợp hiệu suất theo tác giả
CREATE TABLE IF NOT EXISTS agg_author_performance (
    s_key VARCHAR(64) PRIMARY KEY,
    authorID BIGINT,
    authorName VARCHAR(255),
    totalViews BIGINT,
    totalLikes BIGINT,
    totalComments BIGINT,
    totalShares BIGINT,
    totalVideos INT,
    avgViewsPerVideo DECIMAL(10,2)
    );

-- Bảng tổng hợp theo ngày
CREATE TABLE IF NOT EXISTS agg_daily_performance (
    s_key VARCHAR(64) PRIMARY KEY,
    dateKey INT,
    fullDate DATE,
    dayName VARCHAR(32),
    totalViews BIGINT,
    totalLikes BIGINT,
    totalComments BIGINT,
    totalShares BIGINT,
    totalVideos INT
    );
