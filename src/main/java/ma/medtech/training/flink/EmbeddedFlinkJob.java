package ma.medtech.training.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EmbeddedFlinkJob {
    public static void main(String[] args) throws Exception {
        // env.getExecutionEnvironment() est suffisant pour créer un environnement de Stream
        // Nous utilisons ici un cluster embedded pour simuler un cluster Flink
        Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "8081"); // Port Web UI
        config.setString(RestOptions.ADDRESS, "localhost");

        MiniClusterConfiguration miniClusterConfig = new MiniClusterConfiguration.Builder()
            .setNumTaskManagers(1)
            .setNumSlotsPerTaskManager(2)
            .setConfiguration(config)
            .build();

        MiniCluster miniCluster = new MiniCluster(miniClusterConfig);
        miniCluster.start();

        // Créer un environnement de Stream lié au cluster embedded
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
            "localhost",
            8081,
            config
        );

        env.fromElements(1, 2, 3, 4, 5)
            .map(x -> x * 2)
            .print();

        JobExecutionResult result = env.execute("Job en mode embedded");
        System.out.println("Job execution completed with result: " + result.toString());

        miniCluster.close();
    }
}
