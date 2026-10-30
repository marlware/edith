# Edith

Ad decisioning service. Picks a simulated ad for a viewer and records impressions.

- `GET /ads?viewerId=` - select an ad
- `POST /impressions`, `GET /impressions` - record and list impressions

Runs on port 8082: `./mvnw spring-boot:run`
