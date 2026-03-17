package wood.mike.gps;

public record Segment(
        String segmentId,
        double startLat,
        double startLon,
        double endLat,
        double endLon
) {}