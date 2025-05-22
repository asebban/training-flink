package ma.medtech.training.flink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DeduplicationFunction extends KeyedProcessFunction<String, Event, Event> {

        private transient MapState<Double, Long> seenEvents;

    @Override
    public void open(Configuration parameters) {
        seenEvents = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("seenEvents", Double.class, Long.class)
        );
    }

    @Override
    public void processElement(Event event, Context ctx, Collector<Event> out) throws Exception {
        if (!seenEvents.contains(event.amount)) {
            // Événement jamais vu → on l’émet
            out.collect(event);

            // On stocke le montant avec expiration dans 30 secondes
            long cleanupTime = ctx.timerService().currentProcessingTime() + 10_000;
            seenEvents.put(event.amount, cleanupTime);
            ctx.timerService().registerProcessingTimeTimer(cleanupTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Event> out) throws Exception {
        // Supprimer tous les montans expirés
        for (Double amount : seenEvents.keys()) {
            Long expiration = seenEvents.get(amount);
            if (expiration != null && expiration <= timestamp) {
                seenEvents.remove(amount);
            }
        }
    }

}
