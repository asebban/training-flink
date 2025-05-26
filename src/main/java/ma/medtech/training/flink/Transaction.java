package ma.medtech.training.flink;

public class Transaction {
    private String cardNumber;
    private double amount;
    private long timestamp;

    public Transaction() {}

    public Transaction(String cardNumber, double amount, long timestamp) {
        this.cardNumber = cardNumber;
        this.amount = amount;
        this.timestamp = timestamp;
    }

    public String getCardNumber() { return cardNumber; }
    public double getAmount() { return amount; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
        return "Transaction{" +
               "cardNumber='" + cardNumber + '\'' +
               ", amount=" + amount +
               ", timestamp=" + timestamp +
               '}';
    }
}
