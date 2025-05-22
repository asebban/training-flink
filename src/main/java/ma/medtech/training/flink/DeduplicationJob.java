package ma.medtech.training.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class DeduplicationJob {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Event> stream = env.addSource(new GenericSource());

        stream
            .keyBy(event -> event.userId)
            .process(new DeduplicationFunction())
            .print();

        env.execute("Deduplication by user and eventId");
    }
}
