package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class GenericSource implements SourceFunction<Event> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        ctx.collect(new Event("user1", 120.0));
        Thread.sleep(1000); // Simulate delay
        ctx.collect(new Event("user1", 120.0));
        Thread.sleep(1000); // Simulate delay
        ctx.collect(new Event("user1", 200.0));
        Thread.sleep(1000); // Simulate delay
        ctx.collect(new Event("user2", 150.0));
        Thread.sleep(10_000); // Simulate delay
        ctx.collect(new Event("user1", 120.0));
    }

    @Override
    public void cancel() {
        running = false;
    }
}
