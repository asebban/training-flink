package ma.medtech.training.flink;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class TransactionSource implements SourceFunction<Transaction> {
    private volatile boolean running = true;
    private Random random = new Random();
    private String[] cards = {"CARD-1111", "CARD-2222"};

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        long baseTime = System.currentTimeMillis();

        ctx.collectWithTimestamp(new Transaction("CARD-1111", 200, baseTime), baseTime);
        Thread.sleep(100);

        ctx.collectWithTimestamp(new Transaction("CARD-1111", 300, baseTime + 2000), baseTime + 2000);
        Thread.sleep(100);

        ctx.collectWithTimestamp(new Transaction("CARD-1111", 400, baseTime - 5000), baseTime - 5000); // late
        Thread.sleep(100);
    }

    @Override
    public void cancel() {
        running = false;
    }
}
