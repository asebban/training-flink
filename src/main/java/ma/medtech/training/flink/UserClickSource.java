package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class UserClickSource implements SourceFunction<UserClick> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<UserClick> ctx) throws Exception {
        ctx.collectWithTimestamp(new UserClick("alice", 1_000), 1_000);
        ctx.collectWithTimestamp(new UserClick("bob", 2_000), 2_000);
        ctx.collectWithTimestamp(new UserClick("alice", 8_000), 8_000);
        ctx.collectWithTimestamp(new UserClick("bob", 10_500), 10_500); // dans fenêtre suivante
        Thread.sleep(1000);
    }

    @Override
    public void cancel() {
        running = false;
    }
}