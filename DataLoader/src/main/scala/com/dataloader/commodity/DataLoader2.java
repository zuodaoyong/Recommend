package com.dataloader.commodity;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

public class DataLoader2 {


    // 定义数据文件路径
    private static final String PRODUCT_DATA_PATH = "D:\\project\\Recommend\\DataLoader\\src\\main\\resources\\commodity\\products.csv";
    private static final String RATING_DATA_PATH = "D:\\project\\Recommend\\DataLoader\\src\\main\\resources\\commodity\\ratings.csv";
    private static final String mongouri="mongodb://localhost:27017/commodityRecommender";
    private static final String mongodb="commodityRecommender";
    private static final String MONGODB_PRODUCT_COLLECTION = "Product2";
    private static final String MONGODB_RATING_COLLECTION = "Rating2";
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local[*]")
                .setAppName(DataLoader2.class.getSimpleName());
        SparkSession session=SparkSession.builder().config(conf).getOrCreate();
        //加载数据
        Dataset<Product> productDS = getProductDS(session);
        Dataset<Rating> ratingDS = getRatingDS(session);
        MongoClient mongoClient=new MongoClient(new MongoClientURI(mongouri));
        MongoCollection<Document> productCollection = mongoClient.getDatabase(mongodb).getCollection(MONGODB_PRODUCT_COLLECTION);
        MongoCollection<Document> ratingCollection = mongoClient.getDatabase(mongodb).getCollection(MONGODB_RATING_COLLECTION);
        // 如果表已经存在，则删掉
        productCollection.drop();
        ratingCollection.drop();
        productDS.write().option("uri",mongouri)
                .option("collection",MONGODB_PRODUCT_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql").save();
        ratingDS.write().option("uri",mongouri)
                .option("collection",MONGODB_RATING_COLLECTION)
                .mode("overwrite")
                .format("com.mongodb.spark.sql").save();
        session.stop();
    }

    private static Dataset<Rating> getRatingDS(SparkSession session){
        Dataset<String> rating_SourceDS = session.read().textFile(RATING_DATA_PATH);
        return rating_SourceDS.map(new MapFunction<String, Rating>() {
            @Override
            public Rating call(String s) throws Exception {
                String[] split = s.split(",");
                Rating rating=new Rating();
                rating.setUserId(Integer.valueOf(split[0]));
                rating.setProductId(Integer.valueOf(split[1]));
                rating.setScore(Double.valueOf(split[2]));
                rating.setTimestamp(Integer.valueOf(split[3]));
                return rating;
            }
        },Encoders.bean(Rating.class));
    }

    private static Dataset<Product> getProductDS(SparkSession session){
        Dataset<String> product_SourceDS = session.read().textFile(PRODUCT_DATA_PATH);
        return product_SourceDS.map(new MapFunction<String, Product>() {
            @Override
            public Product call(String line) throws Exception {
                String[] split = line.split("\\^");
                Product product = new Product();
                product.setProductId(Integer.valueOf(split[0]));
                product.setName(split[1]);
                product.setImageUrl(split[4]);
                product.setCategories(split[5]);
                product.setTags(split[6]);
                return product;
            }
        }, Encoders.bean(Product.class));
    }
    public static class Product{
       private Integer productId;
       private String name;
       private String imageUrl;
       private String categories;
       private String tags;

        public Integer getProductId() {
            return productId;
        }

        public void setProductId(Integer productId) {
            this.productId = productId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getImageUrl() {
            return imageUrl;
        }

        public void setImageUrl(String imageUrl) {
            this.imageUrl = imageUrl;
        }

        public String getCategories() {
            return categories;
        }

        public void setCategories(String categories) {
            this.categories = categories;
        }

        public String getTags() {
            return tags;
        }

        public void setTags(String tags) {
            this.tags = tags;
        }

        @Override
        public String toString() {
            return "Product{" +
                    "productId=" + productId +
                    ", name='" + name + '\'' +
                    ", imageUrl='" + imageUrl + '\'' +
                    ", categories='" + categories + '\'' +
                    ", tags='" + tags + '\'' +
                    '}';
        }
    }

    public static class Rating{
        private Integer userId;
        private Integer productId;
        private Double score;
        private Integer timestamp;

        public Integer getUserId() {
            return userId;
        }

        public void setUserId(Integer userId) {
            this.userId = userId;
        }

        public Integer getProductId() {
            return productId;
        }

        public void setProductId(Integer productId) {
            this.productId = productId;
        }

        public Double getScore() {
            return score;
        }

        public void setScore(Double score) {
            this.score = score;
        }

        public Integer getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Integer timestamp) {
            this.timestamp = timestamp;
        }
    }
}
