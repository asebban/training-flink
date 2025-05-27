package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class UserClickSource implements SourceFunction<UserClick> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<UserClick> ctx) throws Exception {
        // simulate out-of-order events
        ctx.collect(new UserClick("alice", 1_000L));     // 1s -> first window
        ctx.collect(new UserClick("bob", 3_000L));       // 3s -> first window
        ctx.collect(new UserClick("alice", 8_000L));     // 8s -> first window
        ctx.collect(new UserClick("bob", 10_000L));      // 10s -> second window
        ctx.collect(new UserClick("alice", 13_000L));     // 13s -> second window
        ctx.collect(new UserClick("bob", 15_000L));      // 15s -> second window
        ctx.collect(new UserClick("bob", 7_000L));      // 15s -> second window

        // give Flink time to emit watermarks
        Thread.sleep(1000);
    }

    @Override
    public void cancel() {
        running = false;
    }
}

