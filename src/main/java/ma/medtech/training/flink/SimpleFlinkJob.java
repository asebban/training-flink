package ma.medtech.training.flink;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
@SuppressWarnings("deprecation")
public class SimpleFlinkJob {

    public static void main(String[] args) throws Exception {
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTag<Transaction> lateTag = new OutputTag<Transaction>("late") {};

        DataStream<Transaction> input = env
            .addSource(new TransactionSource())
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                    .withTimestampAssigner((txn, ts) -> txn.getTimestamp())
            );

        SingleOutputStreamOperator<String> processed = input
            .keyBy(Transaction::getCardNumber)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .sideOutputLateData(lateTag)
            .process(new ProcessWindowFunction<Transaction, String, String, TimeWindow>() {
                @Override
                public void process(String key,
                        ProcessWindowFunction<Transaction, String, String, TimeWindow>.Context ctx,
                        Iterable<Transaction> elements, Collector<String> out) throws Exception {
                    double sum = 0.0;
                    for (Transaction txn : elements) {
                        sum += txn.getAmount();
                    }
                    out.collect("Carte " + key + " -> total = " + sum);

                }
            });

        DataStream<Transaction> lateStream = processed.getSideOutput(lateTag);

        // Print the processed results
        processed.print("Processed Transactions");
        lateStream.print("Late Transactions");
        // Execute the job
        env.execute("Simple Flink Job");
    }
}
