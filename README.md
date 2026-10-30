# Edith

Ad decisioning service. Picks a simulated ad for a viewer and records impressions.

- `GET /ads?viewerId=` - select an ad
- `GET /impressions` - list recorded impressions
- Consumes the Kafka `ad-impressions` topic (published by Agnes) to record impressions

Runs on port 8082: `./mvnw spring-boot:run`

## Redis

Targeting data is stored in Redis (`docker compose up -d` in `margo`). Seeded on startup:

- `viewer:{id}` - hash with the viewer's interest (alice = sports, bob = tech)
- `ad:{id}` - hash with advertiser and hlsPath
- `ads:{interest}` - set of ad ids eligible for that interest (unknown viewers get `general`)
