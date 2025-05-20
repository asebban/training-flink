package ma.medtech.training.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageSessionTimeJob
 {
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

        DataStream<Tuple2<String, Integer>> sessions = env.fromElements(Tuple2.of("user1", 5), Tuple2.of("user1", 15), Tuple2.of("user2", 10));
        DataStream<Tuple2<String, Double>> aggregatedSessions = sessions.keyBy(t -> t.f0).window(TumblingProcessingTimeWindows.of(Time.seconds(10))).aggregate(new AverageSessionTimeAggregator());

        JobExecutionResult result = env.execute("Job en mode embedded");
        Thread.sleep(15_000); // Attendre que le job soit lancé

        System.out.println("Job execution completed with result: " + result.toString());

        miniCluster.close();
    }
}
