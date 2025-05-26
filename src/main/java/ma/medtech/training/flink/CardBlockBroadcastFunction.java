package ma.medtech.training.flink;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class CardBlockBroadcastFunction extends BroadcastProcessFunction<Transaction, CardStatus, String> {

    private final MapStateDescriptor<String, String> descriptor;

    public CardBlockBroadcastFunction(MapStateDescriptor<String, String> descriptor) {
        this.descriptor = descriptor;
    }

    @Override
    public void processElement(Transaction txn,
                               ReadOnlyContext ctx,
                               Collector<String> out) throws Exception {
        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(descriptor);
        String status = state.get(txn.getCardNumber());

        if ("BLOCKED".equals(status)) {
            out.collect("FRAUDE : transaction bloquée -> " + txn);
        } else {
            out.collect("Transaction OK : " + txn);
        }
    }

    @Override
    public void processBroadcastElement(CardStatus update,
                                        Context ctx,
                                        Collector<String> out) throws Exception {
        BroadcastState<String, String> state = ctx.getBroadcastState(descriptor);
        state.put(update.getCardNumber(), update.getStatus());

        out.collect("Mise à jour : " + update.getCardNumber() + " -> " + update.getStatus());
    }
}
