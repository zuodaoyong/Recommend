package com.offline.commodity;

import com.dataloader.commodity.DataLoader2;
import com.util.MongoUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.*;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;

public class OffLineRecommend2 {


    private static final String MONGODB_RATING_COLLECTION = "Rating2";
    private static final String USER_RECS = "UserRecs";//用户推荐表
    private static final String PRODUCT_RECS = "ProductRecs";//商品推荐表
    private static final int USER_MAX_RECOMMENDATION = 20;
    private static final String mongouri="mongodb://localhost:27017/commodityRecommender";
    private static final String mongodb="commodityRecommender";
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local[*]").setAppName(OffLineRecommend2.class.getSimpleName());
        SparkSession sparkSession=SparkSession.builder().config(conf).getOrCreate();
        Dataset<DataLoader2.Rating> dataset = MongoUtil.getCollection(mongouri, mongodb, MONGODB_RATING_COLLECTION, sparkSession, DataLoader2.Rating.class);
        JavaRDD<DataLoader2.Rating> ratingJavaRDD = dataset.toJavaRDD();
        ratingJavaRDD.cache();
        JavaRDD<Integer> userIdRDD=ratingJavaRDD.map(new Function<DataLoader2.Rating, Integer>() {
            @Override
            public Integer call(DataLoader2.Rating rating) throws Exception {
                return rating.getUserId();
            }
        }).distinct();
        JavaRDD<Integer> productIdRDD=ratingJavaRDD.map(new Function<DataLoader2.Rating, Integer>() {
            @Override
            public Integer call(DataLoader2.Rating rating) throws Exception {
                return rating.getProductId();
            }
        }).distinct();
        //1. 训练隐语义模型
        JavaRDD<Rating> trainData=ratingJavaRDD.map(new Function<DataLoader2.Rating, Rating>() {
            @Override
            public Rating call(DataLoader2.Rating rating) throws Exception {
                return new Rating(rating.getUserId(),rating.getProductId(),rating.getScore());
            }
        });
        MatrixFactorizationModel model = ALS.train(trainData.rdd(), 5, 10, 0.01);
        // 2. 获得预测评分矩阵，得到用户的推荐列表
        // 用userRDD和productRDD做一个笛卡尔积，得到空的userProductsRDD表示的评分矩阵
        JavaPairRDD<Integer, Integer> cartesian = userIdRDD.cartesian(productIdRDD);
        JavaRDD<Rating> predict = model.predict(cartesian);
        // 从预测评分矩阵中提取得到用户推荐列表
        JavaRDD<Tuple2<Integer, Tuple2<Integer, Double>>> rdd = predict.map(new Function<Rating, Tuple2<Integer, Tuple2<Integer, Double>>>() {
            @Override
            public Tuple2<Integer, Tuple2<Integer, Double>> call(Rating rating) throws Exception {
                Tuple2<Integer, Double> doubleTuple2 = new Tuple2<>(rating.product(), rating.rating());
                return new Tuple2<Integer, Tuple2<Integer, Double>>(rating.user(), doubleTuple2);
            }
        });
        JavaRDD<UserRecs> recsJavaRDD = rdd.groupBy(new Function<Tuple2<Integer, Tuple2<Integer, Double>>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Tuple2<Integer, Double>> integerTuple2Tuple2) throws Exception {
                return integerTuple2Tuple2._1;
            }
        }).map(new Function<Tuple2<Integer, Iterable<Tuple2<Integer, Tuple2<Integer, Double>>>>, UserRecs>() {
            @Override
            public UserRecs call(Tuple2<Integer, Iterable<Tuple2<Integer, Tuple2<Integer, Double>>>> integerIterableTuple2) throws Exception {
                Iterable<Tuple2<Integer, Tuple2<Integer, Double>>> tuple2Iterable = integerIterableTuple2._2;
                Iterator<Tuple2<Integer, Tuple2<Integer, Double>>> tuple2Iterator = tuple2Iterable.iterator();
                List<Tuple2<Integer, Double>> list = new ArrayList<>();
                while (tuple2Iterator.hasNext()) {
                    list.add(tuple2Iterator.next()._2);
                }
                List<Recommendation> recs = list.stream().sorted(Comparator.comparingDouble(new ToDoubleFunction<Tuple2<Integer, Double>>() {
                    @Override
                    public double applyAsDouble(Tuple2<Integer, Double> value) {
                        return value._2;
                    }
                }).reversed()).map(e -> {
                    Recommendation recommendation = new Recommendation();
                    recommendation.setProductId(e._1);
                    recommendation.setScore(e._2);
                    return recommendation;
                }).collect(Collectors.toList());
                recs=recs.subList(0,USER_MAX_RECOMMENDATION);
                UserRecs userRecs = new UserRecs();
                userRecs.setUserId(integerIterableTuple2._1);
                userRecs.setRecs(recs);
                return userRecs;
            }
        });
        Dataset<UserRecs> recsDS = getRecsDS(sparkSession, recsJavaRDD);
        MongoUtil.saveCollection(mongouri,mongodb,USER_RECS,recsDS);
        sparkSession.stop();
    }

    private static Dataset<UserRecs> getRecsDS(SparkSession sparkSession,JavaRDD<UserRecs> recsJavaRDD){
        Encoder<UserRecs> userRecsEncoder = Encoders.bean(UserRecs.class);
        Dataset<UserRecs> dataset = sparkSession.createDataset(recsJavaRDD.rdd(), userRecsEncoder);
        dataset=dataset.repartition(1);
        return dataset;
    }
    /**
     * 定义商品相似度列表
     */
    public static class ProductRecs{
        private int productId;
        private List<Recommendation> recs;

        public int getProductId() {
            return productId;
        }

        public void setProductId(int productId) {
            this.productId = productId;
        }

        public List<Recommendation> getRecs() {
            return recs;
        }

        public void setRecs(List<Recommendation> recs) {
            this.recs = recs;
        }
    }
    /**
     * 定义用户的推荐列表
     */
    public static class UserRecs{
        private int userId;
        private List<Recommendation> recs;

        public int getUserId() {
            return userId;
        }

        public void setUserId(int userId) {
            this.userId = userId;
        }

        public List<Recommendation> getRecs() {
            return recs;
        }

        public void setRecs(List<Recommendation> recs) {
            this.recs = recs;
        }
    }
    /**
     * 定义标准推荐对象
     */
    public static class Recommendation{
        private int productId;
        private double score;

        public int getProductId() {
            return productId;
        }

        public void setProductId(int productId) {
            this.productId = productId;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }
    }
}
