package ma.medtech.training.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.*;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class EmbeddedFlinkWithWebUI {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        // 1. Configuration du cluster avec Web UI activée
        Configuration config = new Configuration();
        config.setBoolean(RestOptions.ENABLE_FLAMEGRAPH, true);
        config.setString(RestOptions.BIND_PORT, "8082-8083"); // Plage de ports pour éviter les conflits
        config.setString(JobManagerOptions.ADDRESS, "localhost");
        config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);

        MiniClusterConfiguration clusterConfig = new MiniClusterConfiguration.Builder()
            .setConfiguration(config)
            .setNumTaskManagers(1)
            .setNumSlotsPerTaskManager(2)
            .build();

        try (MiniCluster cluster = new MiniCluster(clusterConfig)) {
            cluster.start();

            // 3. Création d'un StreamExecutionEnvironment relié au cluster
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
            env.setParallelism(1);

            // 4. Création d’un DataStream simple
            DataStream<String> stream = env.addSource(new GenericSource())
                .map(String::toUpperCase);

            stream.print();

            // 5. Soumission du job
            JobExecutionResult result = env.execute("Flink Embedded WebUI Job");

            System.out.println("Job execution completed with result: " + result.toString());
        }
    }
}
