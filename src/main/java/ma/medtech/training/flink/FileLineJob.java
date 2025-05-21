package ma.medtech.training.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FileLineJob {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> stream = env.addSource(new FileLineSource("data.txt"));

        stream.map(String::toUpperCase)
              .print();

        env.execute("Lecture de fichier ligne par ligne");
    }
}