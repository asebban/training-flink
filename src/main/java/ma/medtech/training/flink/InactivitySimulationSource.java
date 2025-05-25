package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class InactivitySimulationSource implements SourceFunction<String> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (int i = 1; i <= 5 && running; i++) {
            ctx.collect("user-" + i);       // émet la chaîne
            Thread.sleep(2_000);           // attend 1 s
        }
        Thread.sleep(10_000); // attend 10 s pour simuler l'inactivité
        ctx.collect("user-1"); // émet à nouveau pour simuler l'activité
    }
    

    @Override
    public void cancel() {
        running = false;
    }
}
