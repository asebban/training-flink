package ma.medtech.training.flink;

public class Withdrawal {

    public String userId;
    public Double amount;
    public Long timestamp;

    public Withdrawal() {
        // Default constructor for serialization/deserialization
    }

    public Withdrawal(String userId, Double amount, Long timestamp) {
        this.userId = userId;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public Withdrawal(String userId, int amount, Long timestamp) {
        this.userId = userId;
        this.amount = (double) amount;
        this.timestamp = timestamp;
    }

    public String userId() {
        return userId;
    }

    public Double amount() {
        return amount;
    }

    public Long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Withdrawal{" +
                "userId='" + userId + '\'' +
                ", amount=" + amount +
                ", timestamp=" + timestamp +
                '}';
    }

}
