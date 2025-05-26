package ma.medtech.training.flink;

import java.util.Random;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
@SuppressWarnings("deprecation")
public class SimpleFlinkJob {

    // Source Transactions : 1 txn / 500 ms

    public static class TxSource implements SourceFunction<Transaction> {
        private volatile boolean running = true;
        private final Random rnd = new Random();
        @Override public void run(SourceContext<Transaction> ctx) throws Exception {
            int nbTrx = 20;
            while (running && nbTrx-- > 0) {
                String card = "CARD-1";
                double amt  = 100 + rnd.nextDouble() * 900;       // 100–1000
                ctx.collect(new Transaction(card, amt, System.currentTimeMillis()));
                Thread.sleep(1000);
            }
        }
        @Override public void cancel() { running = false; }
    }

    // Source Rules : change la règle toutes les 5 s
    public static class RuleSource implements SourceFunction<Rule> {
        private final Random rnd = new Random();
        @Override public void run(SourceContext<Rule> ctx) throws Exception {
            String card = "CARD-1";
            ctx.collect(new Rule(card, 600, System.currentTimeMillis()));   // règle initiale
            Thread.sleep(5_000);
            ctx.collect(new Rule(card, 400, System.currentTimeMillis()));   // durcissement
            Thread.sleep(8_000);
            ctx.collect(new Rule(card,800, System.currentTimeMillis()));   // assouplissement
        }
        @Override public void cancel() {}
    }

    public static class RuleApplier
        extends CoProcessFunction<Transaction, Rule, String> {

        // state pour stocker la règle courante
        private transient ValueState<Double> currentThreshold;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Double> desc =
                new ValueStateDescriptor<>("threshold", Double.class, 1_000.0); // valeur par défaut
            currentThreshold = getRuntimeContext().getState(desc);
        }

        /* ----------- flux 1 : Transactions ----------- */
        @Override
        public void processElement1(Transaction tx, Context ctx, Collector<String> out) throws Exception {
            double threshold = currentThreshold.value();
            if (tx.getAmount() > threshold) {
                out.collect("ALERT (" + threshold + ") " + tx);
            } else {
                out.collect("OK (" + threshold + ") " + tx);
            }
        }

        /* ----------- flux 2 : Règles ----------- */
        @Override
        public void processElement2(Rule rule, Context ctx, Collector<String> out) throws Exception {
            currentThreshold.update(rule.getThreshold());
            out.collect("🔧 NEW RULE -> threshold = " + rule.getThreshold());
        }
    }


    public static void main(String[] args) throws Exception {
        // Create the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> txs   = env.addSource(new TxSource());
        DataStream<Rule>        rules = env.addSource(new RuleSource());

        DataStream<Transaction> txKeyed = txs.keyBy(Transaction::getCardNumber);
        DataStream<Rule>        ruleKeyed = rules.keyBy(rule -> rule.getCardNumber());

        txKeyed.connect(ruleKeyed)
           .process(new RuleApplier())
           .print();

        // Execute the job
        env.execute("Simple Flink Job");
    }
}
