package com.jn.flink.analysis;

import com.jn.flink.pojo.ItemViewCount;
import com.jn.flink.pojo.KafkaMsg1;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

public class HotItems {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.31.232:19092");
        props.setProperty("group.id", "ecomm");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("auto.offset.reset", "earliest");


        env.addSource(new FlinkKafkaConsumer<String>("UserBehaviors", new SimpleStringSchema(), props))
                .map(data -> {
                    String[] dataArray = data.split(",");
                    return new KafkaMsg1(Long.parseLong(dataArray[0].trim()), Long.parseLong(dataArray[1].trim()), Integer.parseInt(dataArray[2].trim()), dataArray[3].trim(), Long.parseLong(dataArray[4].trim()));
                })
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<KafkaMsg1>() {
//                    @Override
//                    public long extractAscendingTimestamp(KafkaMsg1 msg) {
//                        // 原始数据单位秒，将其转成毫秒
//                        return msg.timestamp * 1000;
//                    }
//                })
                .filter(new FilterFunction<KafkaMsg1>() {
                    @Override
                    public boolean filter(KafkaMsg1 kafkaMsg1) throws Exception {
                        return kafkaMsg1.behavior.equals("PV");
                    }
                })
                .keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new windowFunction())
                .keyBy("windowEnd")
                .process(new TopNHotItems(3))
                .print();

        env.execute("hot item jobs");


    }




    //KafkaMsg1 输入类型
    //Long 累加器ACC类型，保存中间状态
    //Long 输出类型
//    public static class CountAgg implements AggregateFunction<KafkaMsg1, Long, Long> {
//
//        /**
//         * 创建累加器保存中间状态（sum count）
//         * @return
//         */
//        @Override
//        public Long createAccumulator() {
//            return 0L;
//        }
//
//        /**
//         * 将元素添加到累加器并返回新的累加器
//         *
//         * @param msg 输入类型
//         * @param acc 累加器ACC类型
//         *
//         * @return 返回新的累加器
//         */
//        @Override
//        public Long add(KafkaMsg1 msg, Long acc) {
//            return acc + 1;
//        }
//
//
//        /**
//         * 累加器合并
//         *
//         * @param acc1
//         * @param acc2
//         * @return
//         */
//        @Override
//        public Long merge(Long acc1, Long acc2) {
//            return acc1 + acc2;
//        }
//
//        /**
//         * 从累加器提取结果
//         *
//         * @param acc
//         * @return
//         */
//        @Override
//        public Long getResult(Long acc) {
//            return acc;
//        }
//    }
//
//
//    public static class WindowResult implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow>{
//
//        @Override
//        public void apply(Tuple key, TimeWindow window, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
//            Long itemId = ((Tuple1<Long>) key).f0;
//            Long count = iterable.iterator().next();
//            collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
//        }
//    }

    /** 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串 */
    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {

        private final int topSize;

        public TopNHotItems(int topSize) {
            this.topSize = topSize;
        }

        // 用于存储商品与点击数的状态，待收齐同一个窗口的数据后，再触发 TopN 计算
        private ListState<ItemViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 状态的注册
            ListStateDescriptor<ItemViewCount> itemsStateDesc = new ListStateDescriptor<>(
                    "itemState-state",
                    ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemsStateDesc);
        }

        @Override
        public void processElement(
                ItemViewCount input,
                Context context,
                Collector<String> collector) throws Exception {

            // 每条数据都保存到状态中
            itemState.add(input);
            // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
            context.timerService().registerEventTimeTimer(input.windowEnd + 1);
        }

        @Override
        public void onTimer(
                long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 获取收到的所有商品点击量
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount item : itemState.get()) {
                allItems.add(item);
            }
            // 提前清除状态中的数据，释放空间
            itemState.clear();
            // 按照点击量从大到小排序
            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return (int) (o2.viewCount - o1.viewCount);
                }
            });
            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
            for (int i=0;i<topSize;i++) {
                ItemViewCount currentItem = allItems.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.viewCount)
                        .append("\n");
            }
            result.append("====================================\n\n");

            out.collect(result.toString());
        }
    }

    /** 用于输出窗口的结果 */
    public static class windowFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        @Override
        public void apply(
                Tuple key,
                TimeWindow window,  // 窗口
                Iterable<Long> aggregateResult, // 聚合函数的结果，即 count 值
                Collector<ItemViewCount> collector  // 输出类型为 ItemViewCount
        ) throws Exception {
            Long itemId = ((Tuple1<Long>) key).f0;
            Long count = aggregateResult.iterator().next();
            collector.collect(ItemViewCount.of(itemId, window.getEnd(), aggregateResult.iterator().next()));
        }
    }

    /** COUNT 统计的聚合函数实现，每出现一条记录加一 */
    public static class CountAgg implements AggregateFunction<KafkaMsg1, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(KafkaMsg1 userBehavior, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

}





