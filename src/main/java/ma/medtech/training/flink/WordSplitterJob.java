package ma.medtech.training.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordSplitterJob{
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

        DataStream<String> sentences = env.fromElements("flink is fast", "Stream processing is cool");
        DataStream<String> words = sentences.flatMap((String line, Collector<String> out) -> {
            for (String word : line.split(" ")) {
                out.collect(word);
            }
        }).returns(Types.STRING);

        words.print();

        JobExecutionResult result = env.execute("Job en mode embedded");
        System.out.println("Job execution completed with result: " + result.toString());

        miniCluster.close();
    }
}
