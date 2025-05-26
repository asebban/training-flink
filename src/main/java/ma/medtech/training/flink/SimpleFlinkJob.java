package ma.medtech.training.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleFlinkJob {

    private static final Logger logger = LoggerFactory.getLogger(SimpleFlinkJob.class);

    public static void main(String[] args) throws Exception {
        logger.info("Starting the Flink job");

        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Print a simple message to indicate the job is running
        env.fromElements("Hello, Flink!")
           .print();

        logger.debug("Kafka consumer created and configured");
        logger.info("Processing stream started");

        // Execute the job
        env.execute("Simple Flink Job");

        logger.info("Flink job execution completed");
    }
}
