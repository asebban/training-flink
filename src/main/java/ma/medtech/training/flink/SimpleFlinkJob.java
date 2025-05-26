package ma.medtech.training.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@SuppressWarnings("deprecation")
public class SimpleFlinkJob {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // important pour l'observation

        // 1. Source avec timestamps + watermarks
        DataStream<Withdrawal> withdrawalStream = env
                .addSource(new WithdrawalSource())
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy
                        .<Withdrawal>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((record, ts) -> record.timestamp())
                );

        // 2. keyBy obligatoire pour CEP sur utilisateur
        DataStream<Withdrawal> keyedStream = withdrawalStream.keyBy(w -> w.userId());

        // 3. Pattern : 3 retraits consécutifs (pas forcément > 1000 ici)
        Pattern<Withdrawal, ?> pattern = Pattern.<Withdrawal>begin("start")
                .where(SimpleCondition.of(w -> w.amount() > 0)) // Condition de base, on peut affiner plus tard
                .times(3)
                .within(Time.minutes(1));

        // 4. Application du CEP sur le stream keyé
        PatternStream<Withdrawal> patternStream = CEP.pattern(keyedStream, pattern);

        // 5. Traitement du match
        DataStream<String> alerts = patternStream.process(new PatternProcessFunction<Withdrawal, String>() {
            @Override
            public void processMatch(Map<String, List<Withdrawal>> match, Context ctx, Collector<String> out) {
                List<Withdrawal> withdrawals = match.get("start");
                double total = withdrawals.stream().mapToDouble(Withdrawal::amount).sum();
                if (total > 1000) {
                    out.collect("ALERTE - total = " + total + " DH, user = " + withdrawals.get(0).userId());
                }
            }
        });

        // 6. Impression des résultats
        alerts.print();

        // 7. Bonus : voir les événements de base
        withdrawalStream.map(w -> "Événement reçu : " + w).print();

        env.execute("Job CEP - retrait");    }
}
