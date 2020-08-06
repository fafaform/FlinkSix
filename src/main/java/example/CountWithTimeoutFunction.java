package example;

import jdk.nashorn.internal.codegen.types.Type;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.tree.Tree;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class CountWithTimeoutFunction extends KeyedProcessFunction<Tuple, Tuple2<String, String>, Tuple2<String, String>> {
    /**
     * The state that is maintained by this process function
     */

//    private static int TIMEOUT = 600000;
    Logger LOG = LoggerFactory.getLogger(CountWithTimeoutFunction.class);
    private static int MINUTE = 1;

    private ValueState<TreeMap<Long,String>> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor("sumState", Types.MAP(Types.LONG, Types.STRING)));
    }

    @Override
    public void processElement(Tuple2<String, String> value, final Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
        // retrieve the current count
        TreeMap<Long,String> current = state.value();
        if (current == null) {
            current = new TreeMap<Long,String>();
            current.put(ctx.timestamp(), value.f1);
            ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + (60000 * MINUTE));
        }else {
            // update the state's count
            current.put(ctx.timestamp(), value.f1);
        }

        LOG.info(ctx.timestamp() + " ==> " + value.f0 + " : " + value.f1);

        // write the state back
        state.update(current);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
        TreeMap<Long,String> result = state.value();
        String sortText = "";
        for (long timestampKey : result.keySet()){
            sortText += " " + result.get(timestampKey);
        }
        LOG.info(ctx.getCurrentKey().toString().substring(1, ctx.getCurrentKey().toString().length()-1) + " : " + sortText);
        out.collect(new Tuple2<String, String>(ctx.getCurrentKey().toString().substring(1, ctx.getCurrentKey().toString().length()-1), sortText));
        state.clear();
    }
}
