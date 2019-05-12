package com.offline.film

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object OffLineRecommend {

  def main(args: Array[String]): Unit = {
    val config=Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/commodityRecommender",
      "mongo.db" -> "commodityRecommender"
    )
    //创建一个sparkconf配置
    val sparkConf=new SparkConf().setAppName(OffLineRecommend.getClass.getSimpleName).setMaster(config("spark.cores"))
    //创建一个sparkSession
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()

  }
}
