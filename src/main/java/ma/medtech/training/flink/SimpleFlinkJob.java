package ma.medtech.training.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleFlinkJob {

    public static void main(String[] args) throws Exception {
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env
        .fromElements("alice", "bob", "alice", "alice", "bob", "charlie")
        .keyBy(userId -> "one")
        .process(new ListOfUsers())
        .print();

        env.execute("User Event Counter");
    }
}
