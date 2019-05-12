package com.offline.commodity

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ProductRating( userId: Int, productId: Int, score: Double, timestamp: Int )

case class MongoConfig( uri:String,db:String)
// 定义标准推荐对象
case class Recommendation( productId: Int, score: Double )
// 定义用户的推荐列表
case class UserRecs(userId: Int,recs: Seq[Recommendation])
// 定义商品相似度列表
case class ProductRecs(productId:Int,recs:Seq[Recommendation])
object OffLineRecommend {

  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"//用户推荐表
  val PRODUCT_RECS = "ProductRecs"//商品推荐表
  val USER_MAX_RECOMMENDATION = 20
  def main(args: Array[String]): Unit = {
    val config=Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/commodityRecommender",
      "mongo.db" -> "commodityRecommender"
    )
    val sparkConf=new SparkConf().setMaster(config("spark.cores")).setAppName(OffLineRecommend.getClass.getSimpleName)
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )
    //加载数据
    val ratingRDD=sparkSession.read.option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load().as[ProductRating]
      .rdd.map(rating => (rating.userId, rating.productId, rating.score))
      .cache()
    // 提取出所有用户和商品的数据集
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()
    // 核心计算过程
    // 1. 训练隐语义模型
    val trainData = ratingRDD.map(x=>Rating(x._1,x._2,x._3))
    // 定义模型训练的参数，rank隐特征个数，iterations迭代词数，lambda正则化系数
    val ( rank, iterations, lambda ) = ( 5, 10, 0.01 )
    val model = ALS.train( trainData, rank, iterations, lambda )
    // 2. 获得预测评分矩阵，得到用户的推荐列表
    // 用userRDD和productRDD做一个笛卡尔积，得到空的userProductsRDD表示的评分矩阵
    val userProducts = userRDD.cartesian(productRDD)
    val preRating = model.predict(userProducts)
    // 从预测评分矩阵中提取得到用户推荐列表
    val userRecs = preRating.filter(_.rating>0)
      .map(
        rating => ( rating.user, ( rating.product, rating.rating ) )
      )
      .groupByKey()
      .map{
        case (userId, recs) =>
          UserRecs( userId, recs.toList.sortWith(_._2>_._2).take(USER_MAX_RECOMMENDATION).map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()
    writeToMongo(userRecs,USER_RECS)
    sparkSession.stop()
  }

  def writeToMongo(df:DataFrame,collection:String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
