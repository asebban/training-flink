package ma.medtech.training.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DurationJob {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8081); // Port du Dashboard Web si besoin

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

        DataStream<Tuple2<String, Double>> txs = env.fromElements(Tuple2.of("card1", 100.0), Tuple2.of("card1", 200.0), Tuple2.of("card2", 50.0));
        DataStream<Tuple2<String, Double>> txsReduced = txs
            .keyBy(t -> t.f0)
            .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));

        txsReduced.print();

        JobExecutionResult result = env.execute("Job en mode embedded");
        System.out.println("Job execution completed with result: " + result.toString());

        miniCluster.close();
    }
}
