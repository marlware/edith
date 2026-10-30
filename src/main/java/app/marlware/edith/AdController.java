package app.marlware.edith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AdController {

    public record Ad(String id, String advertiser, String hlsPath) {}

    public record Impression(String adId, String viewerId, String assetId) {}

    private final List<Ad> ads = List.of(
            new Ad("ad-1", "Acme Soda", "/hls/ads/acme/index.m3u8"),
            new Ad("ad-2", "Globex Shoes", "/hls/ads/globex/index.m3u8"),
            new Ad("ad-3", "Initech Cloud", "/hls/ads/initech/index.m3u8"));

    private final List<Impression> impressions = new CopyOnWriteArrayList<>();

    @GetMapping("/ads")
    public Ad select(@RequestParam String viewerId) {
        // Stand-in for targeting: pick deterministically per viewer.
        return ads.get(Math.floorMod(viewerId.hashCode(), ads.size()));
    }

    @PostMapping("/impressions")
    public Map<String, Object> record(@RequestBody Impression imp) {
        impressions.add(imp);
        return Map.of("recorded", true, "total", impressions.size());
    }

    @GetMapping("/impressions")
    public List<Impression> impressions() {
        return impressions;
    }
}
