package ma.medtech.training.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class AverageSessionTimeAggregator implements AggregateFunction<Tuple2<String, Integer>, Tuple2<String, Tuple2<Integer, Integer>>, Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Tuple2<Integer, Integer>> createAccumulator() {
        return new Tuple2<>(null, new Tuple2<>(0, 0));
    }

    @Override
    public Tuple2<String, Tuple2<Integer, Integer>> add(Tuple2<String, Integer> value, Tuple2<String, Tuple2<Integer, Integer>> accumulator) {
        String key = value.f0;
        int sum = accumulator.f1.f0 + value.f1;
        int count = accumulator.f1.f1 + 1;
        return new Tuple2<>(key, new Tuple2<>(sum, count));
    }

    @Override
    public Tuple2<String, Double> getResult(Tuple2<String, Tuple2<Integer, Integer>> accumulator) {
        String key = accumulator.f0;
        int sum = accumulator.f1.f0;
        int count = accumulator.f1.f1;
        System.out.println("Calculating average for key: " + key + ", sum: " + sum + ", count: " + count);
        double avg = count == 0 ? 0.0 : (double) sum / count;
        return Tuple2.of(key, avg);
    }

    @Override
    public Tuple2<String, Tuple2<Integer, Integer>> merge(Tuple2<String, Tuple2<Integer, Integer>> a, Tuple2<String, Tuple2<Integer, Integer>> b) {
        String key = a.f0 != null ? a.f0 : b.f0;
        int sum = a.f1.f0 + b.f1.f0;
        int count = a.f1.f1 + b.f1.f1;
        return new Tuple2<>(key, new Tuple2<>(sum, count));
    }
}
