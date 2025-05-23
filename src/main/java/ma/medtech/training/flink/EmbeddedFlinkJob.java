package ma.medtech.training.flink;

import java.time.Duration;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class EmbeddedFlinkJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 1. Simulate a stream of UserClick events with out-of-order arrival
        @SuppressWarnings("deprecation")
        DataStream<UserClick> input = env.addSource(new UserClickSource());
        
        // 2. Watermark Strategy: Allow 5 seconds of lateness
        WatermarkStrategy<UserClick> watermarkStrategy = WatermarkStrategy
        .<UserClick>forBoundedOutOfOrderness(Duration.ofSeconds(5))
        .withTimestampAssigner((event, ingestionTime) -> event.getTimestamp());
        
        // 3. Apply timestamps and watermarks
        DataStream<UserClick> withWatermarks = input.assignTimestampsAndWatermarks(watermarkStrategy);
        
        // 4. Key by user and apply tumbling window of 10s        
        withWatermarks
            .map((UserClick event) -> Tuple2.of(event.getUserId(), 1L))
            .returns(new TypeHint<Tuple2<String, Long>>() {})
            .keyBy(value -> value.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(10)))
            .sum(1)
            .map(windowResult -> "User: " + windowResult.f0 + " - Clicks: " + windowResult.f1)
            .print();
        
        env.execute("Flink Event Time Window with Watermarks");

    }
}
