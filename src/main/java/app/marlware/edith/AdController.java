package app.marlware.edith;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import tools.jackson.databind.json.JsonMapper;

/**
 * Targeting data lives in Redis (seeded by {@link TargetingSeeder}):
 *   viewer:{id}      hash  {interest}
 *   ad:{id}          hash  {advertiser, hlsPath}
 *   ads:{interest}   set   of ad ids eligible for that interest
 */
@RestController
public class AdController {

    static final String IMPRESSIONS_TOPIC = "ad-impressions";
    private static final String DEFAULT_INTEREST = "general";

    public record Ad(String id, String advertiser, String hlsPath) {}

    public record Impression(String adId, String viewerId, String assetId) {}

    private final StringRedisTemplate redis;
    private final List<Impression> impressions = new CopyOnWriteArrayList<>();
    private final JsonMapper json = JsonMapper.builder().build();

    public AdController(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @GetMapping("/ads")
    public Ad select(@RequestParam String viewerId) {
        Object interest = redis.opsForHash().get("viewer:" + viewerId, "interest");
        Set<String> members = redis.opsForSet().members("ads:" + (interest != null ? interest : DEFAULT_INTEREST));
        if (members == null || members.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "no eligible ads");
        }

        // Sorted so the same viewer gets a stable pick.
        List<String> eligible = new ArrayList<>(members);
        eligible.sort(null);
        String adId = eligible.get(Math.floorMod(viewerId.hashCode(), eligible.size()));

        Map<Object, Object> ad = redis.opsForHash().entries("ad:" + adId);
        return new Ad(adId, (String) ad.get("advertiser"), (String) ad.get("hlsPath"));
    }

    // Agnes publishes an impression after each ad plays.
    @KafkaListener(topics = IMPRESSIONS_TOPIC)
    public void record(String message) {
        impressions.add(json.readValue(message, Impression.class));
    }

    // Per-ad counts computed by the Flink job (flink/), stored in Redis.
    @GetMapping("/stats")
    public Map<Object, Object> stats() {
        return redis.opsForHash().entries("impressions:byAd");
    }

    @GetMapping("/impressions")
    public List<Impression> impressions() {
        return impressions;
    }
}
