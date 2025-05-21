package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class GenericSource implements SourceFunction<String> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (int i = 1; i <= 5 && running; i++) {
            ctx.collect("user-" + i);       // émet la chaîne
            Thread.sleep(1_000);           // attend 1 s
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
