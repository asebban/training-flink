package ma.medtech.training.flink;

public class Rule {
    private String cardNumber;
    private double threshold;
    private long timestamp;

    public Rule(String cardNumber, double threshold, long timestamp) {
        this.cardNumber = cardNumber;
        this.threshold = threshold;
        this.timestamp = timestamp;
    }

    public double getThreshold() {
        return threshold;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getCardNumber() {
        return cardNumber;
    }

    @Override
    public String toString() {
        return "Rule{" +
                "threshold=" + threshold +
                ", timestamp=" + timestamp +
                '}';
    }
}
