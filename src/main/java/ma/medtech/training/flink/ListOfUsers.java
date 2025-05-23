package ma.medtech.training.flink;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ListOfUsers extends KeyedProcessFunction<String, String, String> {
  private transient ListState<String> listState;

  @Override
  public void open(Configuration config) {
    listState = getRuntimeContext().getListState(
        new ListStateDescriptor<String>(
            "listOfLast3Users",
            Types.STRING
        )
    );
  }

  @Override
  public void processElement(String userId, Context ctx, Collector<String> out) throws Exception {
    LinkedList <String> list = new LinkedList<>();
    for (String user : listState.get()) {
      list.add(user);
    }
    list.add(userId);
    if (list.size() > 3) {
      list.removeFirst();
    }
    listState.update(list);
    StringBuilder sb = new StringBuilder();
    sb.append("Derniers utilisateurs: [");

    Iterator<String> it = list.iterator();
    while (it.hasNext()) {
      String s = it.next();
      sb.append(s);
      if (it.hasNext()) {
        sb.append(",");
      }
    }
    sb.append("]");
    out.collect(sb.toString());    
  }
}
