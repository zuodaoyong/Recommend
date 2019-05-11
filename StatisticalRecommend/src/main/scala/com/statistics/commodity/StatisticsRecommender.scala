import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Rating( userId: Int, productId: Int, score: Double, timestamp: Int )
case class MongoConfig( uri: String, db: String )

object StatisticsRecommender{
  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val RATE_MORE_PRODUCTS = "RateMoreProducts"//评分更多的商品表（历史热门统计）
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"//最近评分更多的商品表（近期热门统计）
  val AVERAGE_PRODUCTS = "AverageProducts"//每个商品的平均评分统计

  def main(args: Array[String]): Unit = {
    val config=Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://localhost:27017/commodityRecommender",
      "mongo.db" -> "commodityRecommender"
    )
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName(StatisticsRecommender.getClass.getSimpleName)
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )
    // 加载数据
    val ratingDF=sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load().as[Rating].toDF()
    // 创建一张叫ratings的临时表
    ratingDF.createOrReplaceTempView("ratings")
    // 用spark sql去做不同的统计推荐
    // 1. 历史热门商品，按照评分个数统计，productId，count
    val rateMoreProductDF=sparkSession.sql("select productId,count(productId) as count from ratings group by productId order by count desc")
    storeDFInMongoDB(rateMoreProductDF,RATE_MORE_PRODUCTS)
    // 2. 近期热门商品，把时间戳转换成yyyyMM格式进行评分个数统计，最终得到productId, count, yearmonth
    changeDateUDF(sparkSession)
    val rateMoreRecentlyProductsDF =sparkSession.sql("select productId,count(productId) as count,changeDate(timestamp) as yearmonth from ratings group by productId,yearmonth order by yearmonth desc,count desc")
    storeDFInMongoDB(rateMoreRecentlyProductsDF,RATE_MORE_RECENTLY_PRODUCTS)
    // 3. 优质商品统计，商品的平均评分，productId，avg
    val averageProductsDF =sparkSession.sql("select productId,avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB(averageProductsDF,AVERAGE_PRODUCTS)
    sparkSession.stop()

  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
  def changeDateUDF(sparkSession: SparkSession): Unit ={
    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    // 注册UDF，将timestamp转化为年月格式yyyyMM
    sparkSession.udf.register("changeDate", (x: Int)=>simpleDateFormat.format(new Date(x * 1000L)).toInt)
  }
}