package ma.medtech.training.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class InactivityTimer extends KeyedProcessFunction<String, String, String> {
    private ValueState<Long> lastSeen;
    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Long> desc = new ValueStateDescriptor<>("lastSeen", Types.LONG);
        lastSeen = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(String event, Context ctx, Collector<String> out) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        lastSeen.update(now);

        // Enregistrer un timer dans 10 secondes
        ctx.timerService().registerProcessingTimeTimer(now + 10_000);
        out.collect("User " + ctx.getCurrentKey() + " is ACTIVE at " + now);
    }

    @Override
    public void onTimer(long ts, OnTimerContext ctx, Collector<String> out) throws Exception {
        Long last = lastSeen.value();
        if (ts >= last + 10_000) {
            out.collect("User " + ctx.getCurrentKey() + " is INACTIVE since 10s");
        }
    }

}
