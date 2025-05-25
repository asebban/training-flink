package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class CardStatusSource implements SourceFunction<CardStatus> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<CardStatus> ctx) throws Exception {
        ctx.collect(new CardStatus("CARD-1111", "BLOCKED"));
        Thread.sleep(5000);
        ctx.collect(new CardStatus("CARD-1111", "ACTIVE"));
        Thread.sleep(5000);
        ctx.collect(new CardStatus("CARD-2222", "BLOCKED"));
    }

    @Override
    public void cancel() {
        running = false;
    }
}