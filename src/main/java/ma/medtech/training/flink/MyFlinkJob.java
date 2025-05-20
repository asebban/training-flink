package ma.medtech.training.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyFlinkJob {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.fromElements("Hello", "Flink", "Stream")
       .map(String::toUpperCase)
       .print();

    env.execute("Simple Flink Job");
  }
}