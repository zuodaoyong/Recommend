package com.dataloader.film

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//创建case类
case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String,
                 val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)
case class Rating(var uid:Int,var mid:Int,var score:Double,var timestamp:Int)
case class Tag(var uid:Int,var mid:Int,var tag:String,var timestamp:Int)
object DataLoder{
  val MOVIE_DATA_PATH = "F:\\git\\Recommend\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "F:\\git\\Recommend\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "F:\\git\\Recommend\\DataLoader\\src\\main\\resources\\tags.csv"

  def main(args:Array[String]): Unit ={

    val config = Map(
      "spark.cores" -> "local[*]"
    )
    //创建sparkConf
    val sparkConf=new SparkConf().setAppName(DataLoder.getClass.getSimpleName).setMaster(config.get("spark.cores").get)
    val sparkSession=SparkSession.builder().config(sparkConf).getOrCreate()
    //加载数据
    var movieRDD=sparkSession.sparkContext.textFile(MOVIE_DATA_PATH)
    println(movieRDD.first())
  }
}