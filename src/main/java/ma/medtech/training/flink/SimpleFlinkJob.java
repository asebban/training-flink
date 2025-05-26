package ma.medtech.training.flink;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
@SuppressWarnings("deprecation")
public class SimpleFlinkJob {

    public static void main(String[] args) throws Exception {
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        final OutputTag<Transaction> lateTag = new OutputTag<Transaction>("late") {};

        DataStream<Transaction> txStream = env
            .addSource(new TransactionSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((tx, ts) -> tx.getTimestamp())
            );

        SingleOutputStreamOperator<Transaction> main =
            txStream
                .keyBy(Transaction::getCardNumber)
                .process(new LateSplitter());

        // flux principal
        main.map(t -> "ON-TIME -> " + t).print();

        // flux secondaire « late »
        DataStream<Transaction> late = main.getSideOutput(lateTag);
        late.map(t -> "LATE -> " + t).print();


        env.execute("Simple Flink Job");
    }
}
