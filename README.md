# DTWH

Quick start

1. Create a `.env` file with MySQL and Apify settings. Example:

```
MYSQL_USER=user
MYSQL_PASSWORD=dwhtiktok
MYSQL_DATABASE=dwh_tiktok
APIFY_TOKEN=your_apify_token
APIFY_ACTOR=actor_id
```

2. Start services:

```bash
docker compose up -d --build
```

3. To load existing JSON files under `storage/` into the staging schema (`dbStaging`), run the loader service (it can also start with Compose):

```bash
docker compose run --rm loader-staging
```

Staging schema tables
- `dbStaging.Authors(authorID, Name, avatar)`
- `dbStaging.Videos(videoID, authorID, TextContent, Duration, CreateTime, WebVideoUrl)`
- `dbStaging.VideoInteractions(interactionID, videoID, DiggCount, PlayCount, ShareCount, CommentCount, CollectCount)`
