package ma.medtech.training.flink;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import ma.medtech.training.flink.TransactionCountJob.Transaction;

@SuppressWarnings("deprecation")
public class TransactionSource implements SourceFunction<Transaction> {
    private volatile boolean running = true;
    Random rand = new Random();
    String[] cards = {"CARD-001", "CARD-002"};
    String[] merchants = {"MERCHANT-A", "MERCHANT-B"};

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        int maxTransactions = 5; // Limit the number of transactions
        while (running && maxTransactions-- > 0) {
            String card = cards[rand.nextInt(cards.length)];
            String merchant = merchants[rand.nextInt(merchants.length)];
            double amount = rand.nextDouble() * 100;
            long timestamp = System.currentTimeMillis();

            ctx.collect(new Transaction(card, merchant, amount, timestamp));
            Thread.sleep(300);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}