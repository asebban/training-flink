package ma.medtech.training.flink;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CountPerUser extends KeyedProcessFunction<String, String, String> {
  private transient ValueState<Integer> countState;

  @Override
  public void open(Configuration config) {
    countState = getRuntimeContext().getState(
      new ValueStateDescriptor<>("userCount", Types.INT));
  }

  @Override
  public void processElement(String userId, Context ctx, Collector<String> out) throws Exception {
    Integer count = countState.value();
    count = (count == null) ? 1 : count + 1;
    countState.update(count);
    out.collect("User " + userId + " has sent " + count + " events.");
  }
}
