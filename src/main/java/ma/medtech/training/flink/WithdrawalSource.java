package ma.medtech.training.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

@SuppressWarnings("deprecation")
public class WithdrawalSource implements SourceFunction<Withdrawal> {

    private volatile boolean isRunning = true;
    private String[] userIds = {"user-1", "user-2"};

    @Override
    public void run(SourceContext<Withdrawal> ctx) throws Exception {
        Random random = new Random();
        int nbTrx = 10;
        while (isRunning && nbTrx-- > 0) {
            String userId = userIds[random.nextInt(userIds.length)];
            int amount = random.nextInt(1000); // Generate an integer amount
            long timestamp = System.currentTimeMillis();

            Withdrawal withdrawal = new Withdrawal(userId, amount, timestamp);
            ctx.collect(withdrawal);

            Thread.sleep(1000); // Emit one event per second
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
