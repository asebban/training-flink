package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

@SuppressWarnings("deprecation")
public class TransactionSource implements SourceFunction<Transaction> {

    private volatile boolean running = true;
    private final Random random = new Random();

    private final String[] cards = {"CARD-1111", "CARD-2222", "CARD-3333"};

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {

        int ntrx = 100;
        while (running && ntrx-- > 0) {
            String card = cards[random.nextInt(cards.length)];
            double amount = 50 + random.nextDouble() * 950; // Montants entre 50 et 1000
            long timestamp = System.currentTimeMillis();

            ctx.collect(new Transaction(card, amount, timestamp));
            Thread.sleep(1000); // 2 transactions par seconde environ
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
