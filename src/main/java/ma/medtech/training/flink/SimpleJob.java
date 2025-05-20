package ma.medtech.training.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class SimpleJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> text = env.fromElements("apple", "banana", "orange");

    DataStream<String> uppercased = text.map(word -> word.toUpperCase());

    uppercased.print();

    env.execute("Simple Flink Job");
  }
}
