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

## Flink job

`flink/` is a Scala Flink 2.3 job that reads the `ad-impressions` topic and keeps a running count per ad in the Redis hash `impressions:byAd`. `GET /stats` returns the counts.

- Needs sbt and a JDK 17-21 to build (JDK 25 is not supported by the Flink build here)
- The cluster runs from `margo/docker-compose.yml` (UI on http://localhost:8083)
- `flink/submit.sh` builds the jar and submits it
