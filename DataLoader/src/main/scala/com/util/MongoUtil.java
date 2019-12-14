package com.util;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

public class MongoUtil {

    private static MongoClient initMongoClient(String mongouri){
        return new MongoClient(new MongoClientURI(mongouri));
    }

    /**
     * save
     * @param mongouri
     * @param mongodb
     * @param collectionName
     * @param dataset
     * @param <T>
     */
    public static  <T> void saveCollection(String mongouri, String mongodb, String collectionName, Dataset<T> dataset){
        MongoCollection<Document> collection = initMongoClient(mongouri).getDatabase(mongodb).getCollection(collectionName);
        if(collection!=null){
            collection.drop();
        }
        dataset.write().option("uri",mongouri)
                .option("collection",collectionName)
                .mode("overwrite")
                .format("com.mongodb.spark.sql").save();
    }

    /**
     * 读取
     * @param mongouri
     * @param mongodb
     * @param collectionName
     * @param sparkSession
     * @param clazz
     * @param <T>
     * @return
     */
    public static <T> Dataset<T> getCollection(String mongouri, String mongodb, String collectionName, SparkSession sparkSession,Class<T> clazz){
        return sparkSession.read()
                .option("uri", mongouri)
                .option("collection", collectionName)
                .format("com.mongodb.spark.sql")
                .load().as(Encoders.bean(clazz));
    }
}
