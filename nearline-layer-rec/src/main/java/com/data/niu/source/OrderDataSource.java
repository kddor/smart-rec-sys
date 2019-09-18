package com.data.niu.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * Created by apple on 2019/9/14.
 * 实现了一个模拟的数据源，它继承自 RichParallelSourceFunction，它是可以有多个实例的 SourceFunction 的接口。
 * 它有两个方法需要实现，一个是 Run 方法，Flink 在运行时对 Source 会直接调用该方法，该方法需要不断的输出数据，
 * 从而形成初始的流。在 Run 方法的实现中，我们随机的产生商品类别和交易量的记录，然后通过 ctx#collect 方法进行发送。
 * 另一个方法是 Cancel 方法，当 Flink 需要 Cancel Source Task 的时候会调用该方法，
 * 我们使用一个 Volatile 类型的变量来标记和控制执行的状态。
 */
public class OrderDataSource  extends RichParallelSourceFunction<Tuple2<String,Integer>> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
        Random random = new Random();
        while (isRunning) {
            Thread.sleep((getRuntimeContext().getIndexOfThisSubtask() + 1) * 1000 * 5);
            String key = "类别" + (char) ('A' + random.nextInt(3));
            int value = random.nextInt(10) + 1;

            System.out.println(String.format("Emits\t(%s, %d)", key, value));
            ctx.collect(new Tuple2<>(key, value));
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
