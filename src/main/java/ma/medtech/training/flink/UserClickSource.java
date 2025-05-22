package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class UserClickSource implements SourceFunction<UserClick> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<UserClick> ctx) throws Exception {
        // simulate out-of-order events
        ctx.collect(new UserClick("alice", 1_000L));     // 1s
        ctx.collect(new UserClick("bob", 3_000L));       // 3s
        ctx.collect(new UserClick("alice", 8_000L));     // 8s
        ctx.collect(new UserClick("bob", 9_000L));      // 10s
        ctx.collect(new UserClick("alice", 6_000L));     // late

        // give Flink time to emit watermarks
        Thread.sleep(1000);
    }

    @Override
    public void cancel() {
        running = false;
    }
}

