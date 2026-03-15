package wood.mike.product;

public record EnrichedLowStockAlert(
        String productId,
        int stockQuantity,
        String category,
        String supplier,
        Long timestamp
) {}
