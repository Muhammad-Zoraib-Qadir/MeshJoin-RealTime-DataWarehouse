package Models;

public class Product {
    private int productId;
    private String productName;
    private double productPrice;
    private int supplierId;
    private String supplierName;
    private int storeId;
    private String storeName;

    public Product(int productId, String productName, double productPrice, int supplierId, String supplierName, int storeId, String storeName) {
        this.productId = productId;
        this.productName = productName;
        this.productPrice = productPrice;
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.storeId = storeId;
        this.storeName = storeName;
    }

    public int getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public double getProductPrice() {
        return productPrice;
    }

    public int getSupplierId() {
        return supplierId;
    }

    public String getSupplierName() {
        return supplierName;
    }

    public int getStoreId() {
        return storeId;
    }

    public String getStoreName() {
        return storeName;
    }
}
