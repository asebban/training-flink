package ma.medtech.training.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomSinkJob {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.fromElements("apple", "banana", "orange", "lemon", "grape");

        stream.addSink(new CountingSink());

        env.execute("Job avec Sink personnalisé");
    }
}

