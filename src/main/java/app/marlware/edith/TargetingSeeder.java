package app.marlware.edith;

import java.util.Map;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/** Loads a few ads and viewers into Redis on startup. Safe to run repeatedly. */
@Component
public class TargetingSeeder implements ApplicationRunner {

    private final StringRedisTemplate redis;

    public TargetingSeeder(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public void run(ApplicationArguments args) {
        seedAd("ad-1", "Acme Soda", "/hls/ads/acme/index.m3u8", "general", "sports");
        seedAd("ad-2", "Globex Shoes", "/hls/ads/globex/index.m3u8", "general", "sports");
        seedAd("ad-3", "Initech Cloud", "/hls/ads/initech/index.m3u8", "general", "tech");
        redis.opsForHash().put("viewer:alice", "interest", "sports");
        redis.opsForHash().put("viewer:bob", "interest", "tech");
    }

    private void seedAd(String id, String advertiser, String hlsPath, String... interests) {
        redis.opsForHash().putAll("ad:" + id, Map.of("advertiser", advertiser, "hlsPath", hlsPath));
        for (String interest : interests) redis.opsForSet().add("ads:" + interest, id);
    }
}
