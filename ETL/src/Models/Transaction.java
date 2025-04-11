package Models;

import java.sql.Timestamp;

public class Transaction {
    private int orderId;
    private Timestamp orderDate;
    private int productId;
    private int quantityOrdered;
    private int customerId;
    private int timeId;

    public Transaction(int orderId, Timestamp orderDate, int productId, int quantityOrdered, int customerId, int timeId) {
        this.orderId = orderId;
        this.orderDate = orderDate;
        this.productId = productId;
        this.quantityOrdered = quantityOrdered;
        this.customerId = customerId;
        this.timeId = timeId;
    }

    public int getOrderId() {
        return orderId;
    }

    public Timestamp getOrderDate() {
        return orderDate;
    }

    public int getProductId() {
        return productId;
    }

    public int getQuantityOrdered() {
        return quantityOrdered;
    }

    public int getCustomerId() {
        return customerId;
    }

    public int getTimeId() {
        return timeId;
    }
}
