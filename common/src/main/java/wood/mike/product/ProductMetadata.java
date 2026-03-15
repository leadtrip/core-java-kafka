package wood.mike.product;

public record ProductMetadata(String category, String supplier) {
    public static ProductMetadata of (String category, String supplier) {
        return new ProductMetadata(category, supplier);
    }
}
