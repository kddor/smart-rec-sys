package com.data.niu.examples;

import com.data.niu.source.OrderDataSource;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * Created by dataniu on 2019/9/14.
 * desc: 实时订单统计
 * 代码来源于：https://ververica.cn/developers/apache-flink-basic-zero-iii-datastream-api-programming/
 */
public class GroupedProcessingTimeWindowSample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> ds = env.addSource(new OrderDataSource());
        ds.print();

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = ds.keyBy(0);

        keyedStream.sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
        @Override
        public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
            return "";
        }
         }).fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
        @Override
        public HashMap<String, Integer> fold(HashMap<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
            accumulator.put(value.f0, value.f1);
            return accumulator;
        }
    }).addSink(new SinkFunction<HashMap<String, Integer>>() {
        @Override
        public void invoke(HashMap<String, Integer> value, Context context) throws Exception {
            // 每个类型的商品成交量
            System.out.println(value);
            // 商品成交总量
            System.out.println(value.values().stream().mapToInt(v -> v).sum());
            }
        });

        env.execute();
}


}
