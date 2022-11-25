package org.flinkwordcount.consumer;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class WordCountAgg {
    public static class WordHashCountAgg implements AggregateFunction<Tag, HashMap<String,Long>, ArrayList<Tuple2<String,Long>>>{

        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(Tag value, HashMap<String, Long> accumulator) {
            if(accumulator.containsKey(value.tag)){
                Long count = accumulator.get(value.tag);
                accumulator.put(value.tag,count+1);
            }else {
                accumulator.put(value.tag,1L);
            }
            return accumulator;
        }


        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
            ArrayList<Tuple2<String, Long>> result = new ArrayList<>();
            for(String key:accumulator.keySet()){
                result.add(Tuple2.of(key,accumulator.get(key)));
            }
            result.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {

                    return (int)(o2.f1.longValue()-o1.f1.longValue());
                }
            });
            return result;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
            for(String key:b.keySet()){
                if(a.containsKey(key)){
                    Long count = a.get(key);
                    count +=b.get(key);
                    a.put(key,count);
                }else{
                    a.put(key,b.get(key));
                }
            }
            return a;
        }
    }
}
