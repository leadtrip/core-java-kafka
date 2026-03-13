package wood.mike;

import java.util.ArrayList;
import java.util.List;

public interface ProductMetadataHelper {
    default List<ProductMetadata> getProductMetadataList() {
        List<ProductMetadata> list = new ArrayList<>();

        list.add(ProductMetadata.of("Gaming Consoles", "TechWorld"));
        list.add(ProductMetadata.of("Home Office Furniture", "OfficeMax"));
        list.add(ProductMetadata.of("Kitchen Appliances", "ChefPro"));
        list.add(ProductMetadata.of("Wireless Audio", "SoundWave"));
        list.add(ProductMetadata.of("Smart Lighting", "Lumina"));
        list.add(ProductMetadata.of("Outdoor Gear", "PeakPerformance"));
        list.add(ProductMetadata.of("Fitness Trackers", "FitLife"));
        list.add(ProductMetadata.of("Automotive Accessories", "AutoZone"));
        list.add(ProductMetadata.of("Board Games", "FunTimes"));
        list.add(ProductMetadata.of("Art Supplies", "CreativeCanvas"));
        list.add(ProductMetadata.of("Skincare Essentials", "GlowUp"));
        list.add(ProductMetadata.of("Pet Food", "PetPalace"));
        list.add(ProductMetadata.of("Baby Clothing", "TinyTots"));
        list.add(ProductMetadata.of("Books & Media", "ReadMore"));
        list.add(ProductMetadata.of("Gardening Tools", "GreenThumb"));
        list.add(ProductMetadata.of("Laptop Accessories", "TechWorld"));
        list.add(ProductMetadata.of("Camera Equipment", "LensMaster"));
        list.add(ProductMetadata.of("Luxury Watches", "TimeKeeper"));
        list.add(ProductMetadata.of("Musical Instruments", "SoundWave"));
        list.add(ProductMetadata.of("Stationery", "OfficeMax"));
        list.add(ProductMetadata.of("Performance Footwear", "PeakPerformance"));
        list.add(ProductMetadata.of("Camping Furniture", "PeakPerformance"));
        list.add(ProductMetadata.of("Home Security Systems", "Lumina"));
        list.add(ProductMetadata.of("Tools & Hardware", "BuildIt"));
        list.add(ProductMetadata.of("Handbags & Luggage", "StylePoint"));
        list.add(ProductMetadata.of("Jewelry & Charms", "TimeKeeper"));
        list.add(ProductMetadata.of("Coffee & Tea", "ChefPro"));
        list.add(ProductMetadata.of("Bath & Body", "GlowUp"));
        list.add(ProductMetadata.of("Educational Toys", "FunTimes"));
        list.add(ProductMetadata.of("Yoga & Pilates", "FitLife"));
        list.add(ProductMetadata.of("Computer Components", "TechWorld"));
        list.add(ProductMetadata.of("Tableware", "ChefPro"));
        list.add(ProductMetadata.of("Bedding & Linens", "HomeCo"));
        list.add(ProductMetadata.of("Party Supplies", "FunTimes"));
        list.add(ProductMetadata.of("Software Licenses", "TechWorld"));
        list.add(ProductMetadata.of("Drone Accessories", "LensMaster"));
        list.add(ProductMetadata.of("Sportswear", "PeakPerformance"));
        list.add(ProductMetadata.of("Bicycle Parts", "AutoZone"));
        list.add(ProductMetadata.of("Organic Groceries", "ChefPro"));
        list.add(ProductMetadata.of("Craft Beer & Spirits", "BrewMaster"));
        list.add(ProductMetadata.of("Home Decor", "HomeCo"));
        list.add(ProductMetadata.of("Safety Equipment", "BuildIt"));
        list.add(ProductMetadata.of("Medical Supplies", "HealthCare"));
        list.add(ProductMetadata.of("Haircare Products", "GlowUp"));
        list.add(ProductMetadata.of("Virtual Reality Gear", "TechWorld"));
        list.add(ProductMetadata.of("Travel Accessories", "StylePoint"));
        list.add(ProductMetadata.of("Sustainable Textiles", "HomeCo"));
        list.add(ProductMetadata.of("Fishing Gear", "PeakPerformance"));
        list.add(ProductMetadata.of("E-Reader Devices", "ReadMore"));
        list.add(ProductMetadata.of("Workwear", "BuildIt"));

        return list;
    }
}
