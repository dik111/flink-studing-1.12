package org.example.action;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Desription:
 * 1.实时计算出当天零点截止到当前时间的销售总额
 * 2.计算出各个分类的销售top3
 * 3.每秒更新一次统计结果
 *
 * @ClassName DoubleElevenBigScreen
 * @Author Zhanyuwei
 * @Date 2021/2/17 14:35
 * @Version 1.0
 **/
public class DoubleElevenBigScreen {
    public static class MySource implements SourceFunction<Tuple2<String, Double>> {
        private boolean flag = true;
        private String[] categorys = {"女装", "男装","图书", "家电","洗护", "美妆","运动", "游戏","户外", "家具","乐器", "办公"};
        private Random random = new Random();

        @Override
        public void run(SourceContext<Tuple2<String, Double>> ctx) throws Exception {
            while (flag){
                //随机生成分类和金额
                int index = random.nextInt(categorys.length);//[0~length) ==> [0~length-1]
                String category = categorys[index];//获取的随机分类
                double price = random.nextDouble() * 100;//注意nextDouble生成的是[0~1)之间的随机数,*100之后表示[0~100)
                ctx.collect(Tuple2.of(category,price));
                Thread.sleep(20);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

}
