package ma.medtech.training.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleFlinkJob {

    public static void main(String[] args) throws Exception {
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Print a simple message to indicate the job is running
        env.fromElements("Hello, Flink!")
           .print();

        // Execute the job
        env.execute("Simple Flink Job");
    }
}
