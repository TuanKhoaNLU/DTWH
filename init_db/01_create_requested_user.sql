-- Create requested application user and grant database permissions
CREATE USER IF NOT EXISTS 'user'@'%' IDENTIFIED BY 'dwhtiktok';
GRANT ALL PRIVILEGES ON dwh_tiktok.* TO 'user'@'%';
GRANT ALL PRIVILEGES ON dbStaging.* TO 'user'@'%';
FLUSH PRIVILEGES;


