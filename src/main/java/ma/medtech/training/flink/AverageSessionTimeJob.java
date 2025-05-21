package ma.medtech.training.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageSessionTimeJob
 {
    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8081); // Port du Dashboard Web si besoin

        // Utiliser l'environnement local pour le MiniCluster
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> sessions = env.addSource(new GenericSource());

        sessions
            .keyBy(t -> t.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .aggregate(new AverageSessionTimeAggregator()).print();

        JobExecutionResult result = env.execute("Job en mode embedded");

        Thread.sleep(10_000); // Pause pour permettre l'affichage des résultats

        System.out.println("Job execution completed with result: " + result.toString());
    }
}
