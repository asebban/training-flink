package ma.medtech.training.flink;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class TransactionSource implements SourceFunction<Transaction> {

    private volatile boolean running = true;
    private final Random rnd = new Random();

    @Override
    public void run(SourceContext<Transaction> ctx) throws Exception {
        long base = System.currentTimeMillis();

        // 2 on-time events
        ctx.collect(new Transaction("CARD-1111", 200, base));
        Thread.sleep(200);
        ctx.collect(new Transaction("CARD-2222", 350, base + 1_000));

        // 1 late event (5s plus vieux)
        Thread.sleep(200);
        ctx.collect(new Transaction("CARD-1111", 500, base - 5_000));

        // boucle
        int nbTrx = 5;
        while (running && nbTrx-- > 0) {
            // 1 event toutes les 700 ms
            String card = rnd.nextBoolean() ? "CARD-1111" : "CARD-2222"; // 50% de chance
            double amt = 100 + rnd.nextDouble() * 900; // 100 à 1000
            ctx.collect(new Transaction(card, amt, System.currentTimeMillis())); // timestamp courant
            Thread.sleep(700);
        }
    }

    @Override public void cancel() { running = false; }
}
