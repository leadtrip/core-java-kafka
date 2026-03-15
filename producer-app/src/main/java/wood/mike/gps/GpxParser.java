package wood.mike.gps;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class GpxParser {
    private static final XmlMapper xmlMapper = new XmlMapper();

    public static List<GpsPoint> parseGpx(String fileName, String userId) throws Exception {
        List<GpsPoint> points = new ArrayList<>();

        InputStream gpxStream = GpxParser.class.getClassLoader().getResourceAsStream(fileName);
        JsonNode root = xmlMapper.readTree(gpxStream);


        JsonNode trackPoints = root.get("trk").get("trkseg").get("trkpt");

        if (trackPoints.isArray()) {
            for (JsonNode node : trackPoints) {
                double lat = node.get("lat").asDouble();
                double lon = node.get("lon").asDouble();
                double ele = node.get("ele").asDouble();

                String timeStr = node.get("time").asText();
                long epochMillis = Instant.parse(timeStr).toEpochMilli();

                points.add(new GpsPoint(userId, lat, lon, ele, epochMillis));
            }
        }
        return points;
    }
}