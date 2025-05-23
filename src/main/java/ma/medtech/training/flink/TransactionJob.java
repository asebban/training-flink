package ma.medtech.training.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import java.time.Duration;

public class TransactionJob {
    @SuppressWarnings("deprecation")
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

        DataStream<Transaction> transactions = env
            .addSource(new TransactionSource())
            .name("Transaction Source")
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
            );

        transactions
            .keyBy(Transaction::getCardNumber)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .trigger(CountTrigger.of(1))
            .process(new FraudDetectionWindowFunction(1000.0))
            .print();

        JobExecutionResult result = env.execute("Job en mode embedded");

        Thread.sleep(5000); // Attendre 10 secondes pour voir les résultats
        System.out.println("Job execution completed with result: " + result.toString());

        miniCluster.close();
    }
}
