package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LateSplitter
        extends KeyedProcessFunction<String, Transaction, Transaction> {

    public static final OutputTag<Transaction> lateTag = new OutputTag<Transaction>("late") {};
    
    @Override
    public void processElement(Transaction tx,
                               Context ctx,
                               Collector<Transaction> out) throws Exception {

        long wm = ctx.timerService().currentWatermark();   // watermark actuel
        if (tx.getTimestamp() < wm) {
            // événement late
            ctx.output(lateTag, tx);
        } else {
            // événement on-time
            out.collect(tx);
        }
    }
}
