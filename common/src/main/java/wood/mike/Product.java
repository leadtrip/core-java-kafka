package wood.mike;

public record Product(
        String id,
        String name,
        double price,
        int stockQuantity
) { }