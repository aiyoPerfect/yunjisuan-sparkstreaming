package org.flinkwordcount.consumer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class WordCountProcessAllwindow {
    public static class WordCountWindowProcessFunc extends ProcessAllWindowFunction<ArrayList<Tuple2<String,Long>>,String, TimeWindow>{


        @Override
        public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
            ArrayList<Tuple2<String, Long>> list = elements.iterator().next();

            StringBuilder result = new StringBuilder();
            result.append("----------\n");
            for(int i = 0;i<10;i++){
                Tuple2<String,Long> currTuple = list.get(i);
                String info = "No. "+ (i+1)+" " +"tag:" +currTuple.f0+" " +"The num of it is:"+"  "
                        +currTuple.f1 +"\n";
                result.append(info);
            }
            result.append("--------------------\n");

            out.collect(result.toString());
        }
    }
}
