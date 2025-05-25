package ma.medtech.training.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleFlinkJob {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Print a simple message to indicate the job is running
        env.addSource(new InactivitySimulationSource())
            .keyBy(event -> "all") // Key by the event itself (e.g., user ID)
            .process(new InactivityTimer()) // Process the events to detect inactivity
            .name("Inactivity Timer")
            .print();

        // Execute the job
        env.execute("Simple Flink Job");
    }
}
