package com.imdb

import org.apache.spark.sql.types.{FloatType, IntegerType, LongType, StringType, StructField, StructType}

object MovieDataSchemas {

  val titleBasicsSchema = StructType(Array(
    StructField("tconst", StringType, true),
    StructField("titleType", StringType, true),
    StructField("primaryTitle", StringType, true),
    StructField("originalTitle", StringType, true),
    StructField("isAdult", IntegerType, true),
    StructField("startYear", StringType, true),
    StructField("endYear", StringType, true),
    StructField("runtimeMinutes", StringType, true),
    StructField("genres", StringType, true)
  ))

  val titleRatingsSchema = StructType(Array(
    StructField("tconst", StringType, true),
    StructField("averageRating", FloatType, true),
    StructField("numVotes", LongType, true)
  ))

  val titlePrincipalsSchema = StructType(Array(
    StructField("tconst", StringType, true),
    StructField("ordering", IntegerType, true),
    StructField("nconst", StringType, true),
    StructField("category", StringType, true),
    StructField("job", StringType, true),
    StructField("characters", StringType, true)
  ))

  val nameBasicsSchema = StructType(Array(
    StructField("nconst", StringType, true),
    StructField("primaryName", StringType, true),
    StructField("birthYear", StringType, true),
    StructField("deathYear", StringType, true),
    StructField("primaryProfession", StringType, true),
    StructField("knownForTitles", StringType, true)
  ))

}
