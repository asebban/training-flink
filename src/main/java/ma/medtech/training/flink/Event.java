package ma.medtech.training.flink;

public class Event {

    public String userId;
    public Double amount;

    public Event() {}

    public Event(String userId, Double amount) {
        this.userId = userId;
        this.amount = amount;
    }

    @Override
    public String toString() {
        return userId + ":" + amount;
    }

    public String getUserId() {
        return userId;
    }
    public void setUserId(String userId) {
        this.userId = userId;
    }
    public Double getAmount() {
        return amount;
    }
    public void setAmount(Double amount) {
        this.amount = amount;
    }

}
