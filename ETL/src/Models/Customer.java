package Models;

public class Customer {
    private int customerId;
    private String customerName;
    private String gender;

    public Customer(int customerId, String customerName, String gender) {
        this.customerId = customerId;
        this.customerName = customerName;
        this.gender = gender;
    }

    public int getCustomerId() {
        return customerId;
    }

    public String getCustomerName() {
        return customerName;
    }

    public String getGender() {
        return gender;
    }
}
