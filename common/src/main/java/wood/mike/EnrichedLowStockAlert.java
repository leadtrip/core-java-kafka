package wood.mike;

public record EnrichedLowStockAlert(
        String productId,
        int stockQuantity,
        String category,
        String supplier,
        Long timestamp
) {}
