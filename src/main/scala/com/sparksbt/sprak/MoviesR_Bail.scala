package com.sparksbt.sprak
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.log4j._

object MoviesR_Bail {

  case class movies(userID : Int, movieID : Int, rating : Int, timestamp: Long)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = new SparkSession.Builder().appName("Movies").master("local").getOrCreate()

    import spark.implicits._
    val movieSchema = new StructType()
      .add("userID", IntegerType)
      .add("movieID", IntegerType)
      .add("rating", IntegerType)
      .add("timestamp", LongType)

    val movieSD = spark.read.option("sep","\t")
      .schema(movieSchema)
      .csv("C:/DEV/SparkScalaCourse/data/ml-100k/u.data").as[movies]
    movieSD.cache()

    val User2Movies = movieSD.as("movieSD1")
      .join(movieSD.as("movieSD2"),
        col("movieSD1.userID") === col("movieSD1.userID"),
        "inner")
      .filter(col("movieSD1.movieID")=!= col("movieSD2.movieID"))
      .select(col("movieSD1.userID").as("userID"),
        (when(col("movieSD1.movieID")>col("movieSD2.movieID"),
          col("movieSD2.movieID")).otherwise(col("movieSD1.movieID"))).as("movieID1"),
        (when(col("movieSD1.movieID")>col("movieSD2.movieID"),
          col("movieSD2.rating")).otherwise(col("movieSD1.rating"))).as("rating1"),
        (when(col("movieSD1.movieID")>col("movieSD2.movieID"),
          col("movieSD1.movieID")).otherwise(col("movieSD2.movieID"))).as("movieID2"),
        (when(col("movieSD1.movieID")>col("movieSD2.movieID"),
          col("movieSD1.rating")).otherwise(col("movieSD2.rating"))).as("rating2"))
      .distinct()

    val MoviesSim = User2Movies.groupBy("movieId1","movieId2")
      .agg(sum((col("rating1") + col("rating2"))/2).as("Similarity")
        ,count("*").as("RatingNumber"))

    val MoviesSimCal = MoviesSim.select(col("movieId1"),col("movieId2")
      ,(round(col("Similarity")/col("RatingNumber"),0))
        .cast("int").as("SimilarityRating")
      ,col("RatingNumber"))
    val Similarity = MoviesSimCal.orderBy(desc("RatingNumber"), desc("SimilarityRating"))

    //print(Similarity.show(30))
    //result.show(10)

println("New modification")


    spark.stop()

  }




}
