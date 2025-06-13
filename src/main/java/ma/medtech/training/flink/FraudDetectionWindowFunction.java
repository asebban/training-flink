package ma.medtech.training.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class FraudDetectionWindowFunction extends ProcessWindowFunction<Transaction, String, String, TimeWindow> {

    private Double maxAmount = 0.0;
    private transient ValueState<Double> state;

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

        if (state == null) {
            @SuppressWarnings("deprecation")
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "totalAmount",
                    Double.class,
                    0.0 // Default value
            );
            state = ctx.globalState().getState(descriptor);
        }
        
        Double amount = 0.0;
        // get the last element of the iterable
        Transaction lastTransaction = null;
        for (Transaction txn : transactions) {
            amount += txn.getAmount();
            lastTransaction = txn; // This will be the last transaction in the iterable
        }
        
        if (lastTransaction != null) {
            System.out.println("Last received Amount for card " + cardNumber + ": " + lastTransaction.getAmount());
        } else {
            System.out.println("No transactions found for card " + cardNumber);
        }
        
        long start = ctx.window().getStart();
        long end = ctx.window().getEnd();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());
        String startFormatted = formatter.format(Instant.ofEpochMilli(start));
        String endFormatted = formatter.format(Instant.ofEpochMilli(end));

        if (amount >= this.maxAmount) {
            // Send an alert or take action
            out.collect("Window -> " + startFormatted + "-" + endFormatted + " : ***** Fraud detected for card " + cardNumber + " with amount " + amount + " greater than " + this.maxAmount + "******");
        } else {
            // No fraud detected
            out.collect("Window -> " + startFormatted + "-" + endFormatted + " : No fraud detected for card " + cardNumber + " with amount " + amount);
        }

    }

    @Override
    public void clear(Context context) throws Exception {
        if (state != null) {
            state.clear(); // Clear the state when the window is closed
        }
        System.out.println("State cleared for window: " + context.window());
    }
}
