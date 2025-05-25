package ma.medtech.training.flink;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SimpleFlinkJob {

    @SuppressWarnings("deprecation")
    public static void main(String[] args) throws Exception {
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<Transaction> transactions = env
        .addSource(new TransactionSource())
        .name("Transaction Source");

        DataStream<CardStatus> blockedCards = env
        .addSource(new CardStatusSource())
        .name("Blocked Cards Source");

        MapStateDescriptor<String, String> cardStateDesc =
            new MapStateDescriptor<>("blocked-cards", String.class, String.class);
        
        transactions
        .connect(blockedCards.broadcast(cardStateDesc))
        .process(new CardBlockBroadcastFunction(cardStateDesc))
        .print();

        // Execute the job
        env.execute("Simple Flink Job");
    }
}
