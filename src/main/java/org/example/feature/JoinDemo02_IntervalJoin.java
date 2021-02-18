package org.example.feature;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Desription:演示flink双流join-IntervalJoin
 *
 * @ClassName JoinDemo02_IntervalJoin
 * @Author Zhanyuwei
 * @Date 2021/2/17 18:11
 * @Version 1.0
 **/
public class JoinDemo02_IntervalJoin {

    public static void main(String[] args) throws Exception {
        // 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        // source
        // 商品数据流
        DataStream<Goods> goodsDS = env.addSource(new GoodsSource11(), TypeInformation.of(Goods.class)).assignTimestampsAndWatermarks(new GoodsWatermark());
        // 订单数据流
        DataStream<OrderItem> orderItemDS = env.addSource(new OrderItemSource(), TypeInformation.of(OrderItem.class)).assignTimestampsAndWatermarks(new OrderItemWatermark());


        // transformation
        SingleOutputStreamOperator<FactOrderItem> resultDS = goodsDS.keyBy(Goods::getGoodsId)
                .intervalJoin(orderItemDS.keyBy(OrderItem::getGoodsId))
                .between(Time.seconds(-2), Time.seconds(1))
                .process(new ProcessJoinFunction<Goods, OrderItem, FactOrderItem>() {
                    @Override
                    public void processElement(Goods left, OrderItem right, Context ctx, Collector<FactOrderItem> out) throws Exception {
                        FactOrderItem result = new FactOrderItem();
                        result.setGoodsId(left.getGoodsId());
                        result.setGoodsName(left.getGoodsName());
                        result.setCount(new BigDecimal(right.getCount()));
                        result.setTotalMoney(new BigDecimal(right.getCount()).multiply(left.getGoodsPrice()));
                        out.collect(result);
                    }
                });

        // sink
        resultDS.print();

        env.execute();
    }

    //商品类
    @Data
    public static class Goods {
        private String goodsId;
        private String goodsName;
        private BigDecimal goodsPrice;

        public static List<Goods> GOODS_LIST;
        public static Random r;

        static  {
            r = new Random();
            GOODS_LIST = new ArrayList<>();
            GOODS_LIST.add(new Goods("1", "小米12", new BigDecimal(4890)));
            GOODS_LIST.add(new Goods("2", "iphone12", new BigDecimal(12000)));
            GOODS_LIST.add(new Goods("3", "MacBookPro", new BigDecimal(15000)));
            GOODS_LIST.add(new Goods("4", "Thinkpad X1", new BigDecimal(9800)));
            GOODS_LIST.add(new Goods("5", "MeiZu One", new BigDecimal(3200)));
            GOODS_LIST.add(new Goods("6", "Mate 40", new BigDecimal(6500)));
        }

        public static Goods randomGoods() {
            int rIndex = r.nextInt(GOODS_LIST.size());
            return GOODS_LIST.get(rIndex);
        }

        public Goods() {
        }

        public Goods(String goodsId, String goodsName, BigDecimal goodsPrice) {
            this.goodsId = goodsId;
            this.goodsName = goodsName;
            this.goodsPrice = goodsPrice;
        }

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }

    //订单明细类
    @Data
    public static class OrderItem {
        private String itemId;
        private String goodsId;
        private Integer count;

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }

    //关联结果
    @Data
    public static class FactOrderItem {
        private String goodsId;
        private String goodsName;
        private BigDecimal count;
        private BigDecimal totalMoney;
        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }
    //构建一个商品Stream源（这个好比就是维表）
    public static class GoodsSource11 extends RichSourceFunction {
        private Boolean isCancel;
        @Override
        public void open(Configuration parameters) throws Exception {
            isCancel = false;
        }
        @Override
        public void run(SourceContext sourceContext) throws Exception {
            while(!isCancel) {
                Goods.GOODS_LIST.stream().forEach(goods -> sourceContext.collect(goods));
                TimeUnit.SECONDS.sleep(1);
            }
        }
        @Override
        public void cancel() {
            isCancel = true;
        }
    }
    //构建订单明细Stream源
    public static class OrderItemSource extends RichSourceFunction {
        private Boolean isCancel;
        private Random r;
        @Override
        public void open(Configuration parameters) throws Exception {
            isCancel = false;
            r = new Random();
        }
        @Override
        public void run(SourceContext sourceContext) throws Exception {
            while(!isCancel) {
                Goods goods = Goods.randomGoods();
                OrderItem orderItem = new OrderItem();
                orderItem.setGoodsId(goods.getGoodsId());
                orderItem.setCount(r.nextInt(10) + 1);
                orderItem.setItemId(UUID.randomUUID().toString());
                sourceContext.collect(orderItem);
                orderItem.setGoodsId("111");
                sourceContext.collect(orderItem);
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isCancel = true;
        }
    }
    //构建水印分配器（此处为了简单），直接使用系统时间了
    public static class GoodsWatermark implements WatermarkStrategy<Goods> {

        @Override
        public TimestampAssigner<Goods> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> System.currentTimeMillis();
        }

        @Override
        public WatermarkGenerator<Goods> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<Goods>() {
                @Override
                public void onEvent(Goods event, long eventTimestamp, WatermarkOutput output) {
                    output.emitWatermark(new Watermark(System.currentTimeMillis()));
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    output.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
            };
        }
    }

    public static class OrderItemWatermark implements WatermarkStrategy<OrderItem> {
        @Override
        public TimestampAssigner<OrderItem> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> System.currentTimeMillis();
        }
        @Override
        public WatermarkGenerator<OrderItem> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<OrderItem>() {
                @Override
                public void onEvent(OrderItem event, long eventTimestamp, WatermarkOutput output) {
                    output.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    output.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
            };
        }
    }

}
