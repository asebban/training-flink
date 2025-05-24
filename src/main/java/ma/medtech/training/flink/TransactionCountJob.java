package ma.medtech.training.flink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TransactionCountJob {

    // Add JVM option to allow java.base access by unnamed modules
    // This can be done by adding the following JVM argument when running the application:
    // --add-modules java.base

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generate a stream of random transactions
        DataStream<Transaction> transactions = env.addSource(new TransactionSource());

        // Process transactions keyed by cardNumber
        transactions
            .keyBy(Transaction::getCardNumber)
            .process(new TransactionCountProcessFunction())
            .print();

        env.execute("Transaction Count Job");
    }

    public static class Transaction {
        private String cardNumber;
        private String merchantId;
        private double amount;
        private long timestamp;

        public Transaction() {}

        public Transaction(String cardNumber, String merchantId, double amount, long timestamp) {
            this.cardNumber = cardNumber;
            this.merchantId = merchantId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        public String getCardNumber() {
            return cardNumber;
        }

        public String getMerchantId() {
            return merchantId;
        }

        public double getAmount() {
            return amount;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Transaction{" +
                    "cardNumber='" + cardNumber + '\'' +
                    ", merchantId='" + merchantId + '\'' +
                    ", amount=" + amount +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    public static class TransactionCountProcessFunction extends KeyedProcessFunction<String, Transaction, String> {

        private transient MapState<String, Integer> merchantTransactionCounts;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            MapStateDescriptor<String, Integer> descriptor = new MapStateDescriptor<>(
                "merchantTransactionCounts",
                Types.STRING,
                Types.INT);
            merchantTransactionCounts = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Transaction transaction, Context context, Collector<String> out) throws Exception {
            String merchantId = transaction.getMerchantId();
            Integer currentCount = merchantTransactionCounts.get(merchantId);
            if (currentCount == null) {
                currentCount = 0;
            }
            merchantTransactionCounts.put(merchantId, currentCount + 1);

            Map<String, Integer> currentState = new HashMap<>();
            for (Map.Entry<String, Integer> entry : merchantTransactionCounts.entries()) {
                currentState.put(entry.getKey(), entry.getValue());
            }

            out.collect("Carte: " + context.getCurrentKey() + " " + currentState);
        }
    }

    @SuppressWarnings("deprecation")
    public static class GenericSource implements SourceFunction<Transaction> {

        private boolean running = true;

        @Override
        public void run(SourceContext<Transaction> ctx) throws Exception {
            Random random = new Random();
            String[] cardNumbers = {"CARD-001", "CARD-002", "CARD-003"};
            String[] merchantIds = {"MERCHANT-A", "MERCHANT-B", "MERCHANT-C"};

            while (running) {
                String cardNumber = cardNumbers[random.nextInt(cardNumbers.length)];
                String merchantId = merchantIds[random.nextInt(merchantIds.length)];
                double amount = random.nextDouble() * 100;
                long timestamp = System.currentTimeMillis();

                ctx.collect(new Transaction(cardNumber, merchantId, amount, timestamp));

                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
