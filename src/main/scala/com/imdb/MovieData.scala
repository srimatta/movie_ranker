package com.imdb

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

object MovieData {

  def readTitleBasics(spark: SparkSession, dataPath: String): DataFrame = {

    spark.read
      .option("sep", "\t")
      .schema(MovieDataSchemas.titleBasicsSchema)
      .csv(dataPath + "title.basics.tsv")
      .filter(col("titleType") === "movie")
      .select("tconst", "primaryTitle")
  }

  def readTitleRatings(spark: SparkSession, dataPath: String, numVotes: Int = 500): DataFrame = {
    spark.read
      .option("sep", "\t")
      .schema(MovieDataSchemas.titleRatingsSchema)
      .csv(dataPath + "title.ratings.tsv")
      .filter(col("numVotes") >= numVotes)
  }

  def readTitlePrincipals(spark: SparkSession, dataPath: String, top10Tconst: Seq[String]): DataFrame = {
    spark.read
      .option("sep", "\t")
      .schema(MovieDataSchemas.titlePrincipalsSchema)
      .csv(dataPath + "title.principals.tsv")
      .filter(col("tconst").isin(top10Tconst: _*))
  }

  def readNameBasics(spark: SparkSession, dataPath: String): DataFrame = {
    spark.read
      .option("sep", "\t")
      .schema(MovieDataSchemas.nameBasicsSchema)
      .csv(dataPath + "name.basics.tsv")
      .select("nconst", "primaryName")
  }
}
