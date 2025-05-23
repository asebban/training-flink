package ma.medtech.training.flink;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.Collector;

public class WindowClickCountJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableGenericTypes();

        @SuppressWarnings("deprecation")
        DataStream<UserClick> input = env.addSource(new UserClickSource());

        WatermarkStrategy<UserClick> watermarkStrategy = WatermarkStrategy
            .<UserClick>forBoundedOutOfOrderness(Duration.ofSeconds(3))
            .withTimestampAssigner((event, ts) -> event.getTimestamp());

        input
            .assignTimestampsAndWatermarks(watermarkStrategy)
            .keyBy(UserClick::getUser)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .apply(new WindowFunction<UserClick, String, String, TimeWindow>() {
                @SuppressWarnings("unused")
                @Override
                public void apply(String user, TimeWindow window, Iterable<UserClick> input, Collector<String> out) {
                    int count = 0;
                    for (UserClick e : input) count++;
                    out.collect(String.format("User: %s | Count: %d | Window: [%d → %d)",
                        user, count, window.getStart(), window.getEnd()));
                }
            })
            .print();

        env.execute("WindowFunction UserClick Example");
    }
}