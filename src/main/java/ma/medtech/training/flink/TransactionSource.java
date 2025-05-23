package ma.medtech.training.flink;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class TransactionSource implements SourceFunction<Transaction> {
    private volatile boolean running = true;
    Random rand = new Random();

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        for (int i = 0; i < 25 && running; i++) {
            String card = (i % 2 == 0) ? "CARD-1111" : "CARD-2222";
            double amount = rand.nextInt(900) + 100;  // [100–1000]
            long ts = System.currentTimeMillis();
            ctx.collect(new Transaction(card, amount, ts));
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
