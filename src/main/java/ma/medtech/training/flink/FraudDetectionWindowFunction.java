package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FraudDetectionWindowFunction extends ProcessWindowFunction<Transaction, String, String, TimeWindow> {

    private Double maxAmount = 0.0;

    public FraudDetectionWindowFunction() {
        super();
    }

    public FraudDetectionWindowFunction(Double maxAmount) {
        this.maxAmount = maxAmount;
    }

    @Override
    public void process(String cardNumber,
                        Context ctx,
                        Iterable<Transaction> transactions,
                        Collector<String> out) throws Exception {
        
        Double amount = 0.0;
        for (Transaction txn : transactions) {
            amount += txn.getAmount();
        }
        System.out.println("Card Number: " + cardNumber + ", Amount: " + amount);

        if (amount >= this.maxAmount) {
            // Send an alert or take action
            out.collect("Fraud detected for card " + cardNumber + " with amount " + amount + "greater than " + this.maxAmount);
        } else {
            // No fraud detected
            out.collect("No fraud detected for card " + cardNumber + " with amount " + amount);
        }

    }
}
