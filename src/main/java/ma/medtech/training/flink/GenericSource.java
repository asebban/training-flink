package ma.medtech.training.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

@SuppressWarnings("deprecation")
public class GenericSource implements SourceFunction<Tuple2<String, Integer>> {

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        int i = 0;
        Integer duration1, duration2;
        while (running) {
            duration1 = 5 + (i % 10);
            duration2 = 10 + (i % 5);
            ctx.collect(Tuple2.of("user1", duration1));
            ctx.collect(Tuple2.of("user2", duration2));
            i++;
            System.out.println("Emitting events: (user1," + duration1 + "), (user2," + duration2 + ")");
            Thread.sleep(1000); // 1 seconde entre chaque lot d'événements
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
