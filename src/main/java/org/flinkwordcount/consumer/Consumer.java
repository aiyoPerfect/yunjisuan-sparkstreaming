package org.flinkwordcount.consumer;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        Properties properties = new Properties();

        //set bootstrap
        properties.put("bootstrap.servers","worker1:9092");

        //
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        //create a consumer
        DataStreamSource<String> kafkaDatastream = env.addSource(new FlinkKafkaConsumer<String>("wordcount", new SimpleStringSchema(), properties));

        //consume
        //创建二元组，进行一个flatmap的扁平化映射， word 对应 一个1-


        SingleOutputStreamOperator<Tag> wordStream = kafkaDatastream.flatMap(new FlatMapFunction<String, Tag>() {
            @Override
            public void flatMap(String value, Collector<Tag> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tag(word, System.currentTimeMillis()));
                }
            }
        });


        SingleOutputStreamOperator<Tag> wordStreamwithStamp = wordStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Tag>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tag>() {
                    @Override
                    public long extractTimestamp(Tag element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));


        wordStreamwithStamp.windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .aggregate(new WordCountAgg.WordHashCountAgg(),new WordCountProcessAllwindow.WordCountWindowProcessFunc())
                .print();

        env.execute();

    }
}
