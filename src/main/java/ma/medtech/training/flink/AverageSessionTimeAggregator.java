package ma.medtech.training.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class AverageSessionTimeAggregator implements AggregateFunction<Tuple2<String, Integer>, Tuple2<Integer, Integer>, Tuple2<String, Double>> {
    @Override
    public Tuple2<Integer, Integer> createAccumulator() {
        return new Tuple2<>(0, 0);
    }

    @Override
    public Tuple2<Integer, Integer> add(Tuple2<String, Integer> value, Tuple2<Integer, Integer> accumulator) {
        Tuple2<Integer, Integer> result = new Tuple2<>(accumulator.f0 + value.f1, accumulator.f1 + 1);
        return result;
    }

    @Override
    public Tuple2<String, Double> getResult(Tuple2<Integer, Integer> accumulator) {
        return Tuple2.of("avg", (double) accumulator.f0 / accumulator.f1);
    }

    @Override
    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }

}
