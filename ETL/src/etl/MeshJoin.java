package etl;


import Models.Customer;
import Models.Product;
import Models.Transaction;

import java.util.*;

public class MeshJoin {
    private final List<Product> md; // Disk-buffer
    private final Queue<Transaction> streamBuffer; // Stream buffer
    private final Queue<Transaction> queue; // Queue
    private final Map<Integer, Customer> hashTable; // Hash table for customers

    public MeshJoin(List<Product> md, List<Transaction> transactions) {
        this.md = md;
        this.streamBuffer = new LinkedList<>(transactions);
        this.queue = new LinkedList<>();
        this.hashTable = new HashMap<>();
    }

    public void performJoin() {
        while (!streamBuffer.isEmpty()) {
            Transaction t = streamBuffer.poll();
            queue.add(t);
            hashTable.put(t.getCustomerId(), fetchCustomer(t.getCustomerId()));

            for (Product product : md) {
                for (Transaction trans : queue) {
                    if (trans.getProductId() == product.getProductId()) {
                        System.out.println("Join Output: OrderID: " + trans.getOrderId() + " -> Product: " + product.getProductName());
                    }
                }
            }

            if (queue.size() > md.size()) {
                Transaction removed = queue.poll();
                hashTable.remove(removed.getCustomerId());
            }
        }
    }

    private Customer fetchCustomer(int customerId) {
        // Mock fetch, replace with real DB fetch if required
        return new Customer(customerId, "Mock Name", "Mock Gender");
    }
}
