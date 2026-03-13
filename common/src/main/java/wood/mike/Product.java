package wood.mike;

public record Product(
        String id,
        String name,
        double price,
        int stockQuantity,
        String categoryId
) {
    public static Product of(String id, String name, double price, int stockQuantity, String categoryId) {
        return new Product(id, name, price, stockQuantity, categoryId);
    }
}