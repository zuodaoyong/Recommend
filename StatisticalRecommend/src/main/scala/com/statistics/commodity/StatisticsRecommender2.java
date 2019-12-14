package com.statistics.commodity;

import com.dataloader.commodity.DataLoader2;
import com.udf.DateUDF;
import com.util.MongoUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StatisticsRecommender2 {
    // 定义mongodb中存储的表名
    private static final String MONGODB_RATING_COLLECTION = "Rating2";
    private static final String RATE_MORE_PRODUCTS = "RateMoreProducts2";//评分更多的商品表（历史热门统计）
    private static final String RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts2";//最近评分更多的商品表（近期热门统计）
    private static final String AVERAGE_PRODUCTS = "AverageProducts2";//每个商品的平均评分统计
    private static final String mongouri="mongodb://localhost:27017/commodityRecommender";
    private static final String mongodb="commodityRecommender";
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName(StatisticsRecommender2.class.getSimpleName())
                .setMaster("local[*]");
        SparkSession sparkSession=SparkSession.builder().config(conf).getOrCreate();
        Dataset<DataLoader2.Rating> ratingDataset = MongoUtil.getCollection(mongouri, mongodb, MONGODB_RATING_COLLECTION, sparkSession, DataLoader2.Rating.class);
        ratingDataset.createOrReplaceTempView("Rating2");
        //1、历史热门商品，按照评分个数统计，productId，count
        historyHot(sparkSession);
        //2、近期热门商品，把时间戳转换成yyyyMM格式进行评分个数统计，最终得到productId, count, yearmonth
        nearFutureHot(sparkSession);
        //3、优质商品统计，商品的平均评分，productId，avg
        highGood(sparkSession);
        sparkSession.stop();
    }

    /**
     * 优质商品统计
     * @param sparkSession
     */
    private static void highGood(SparkSession sparkSession){
        Dataset<Row> dataset = sparkSession.sql("select productId,avg(score) as avg from Rating2 group by productId order by avg desc");
        dataset=dataset.repartition(1);
        MongoUtil.saveCollection(mongouri,mongodb,AVERAGE_PRODUCTS,dataset);
    }

    /**
     * 近期热门商品
     * @param sparkSession
     */
    private static void nearFutureHot(SparkSession sparkSession){
        DateUDF.format_yyyyMM(sparkSession);
        Dataset<Row> dataset = sparkSession.sql("select productId,count(productId) as count,yyyyMM(timestamp) as yearmonth from Rating2 group by productId,yearmonth order by yearmonth desc,count desc");
        dataset=dataset.repartition(1);
        MongoUtil.saveCollection(mongouri,mongodb,RATE_MORE_RECENTLY_PRODUCTS,dataset);
    }

    /**
     * 历史热门商品
     * @param sparkSession
     */
    private static void historyHot(SparkSession sparkSession){
        Dataset<Row> dataset = sparkSession.sql("select productId,count(productId) as count from Rating2 group by productId order by count desc");
        dataset=dataset.repartition(1);
        MongoUtil.saveCollection(mongouri,mongodb,RATE_MORE_PRODUCTS,dataset);
    }
}
