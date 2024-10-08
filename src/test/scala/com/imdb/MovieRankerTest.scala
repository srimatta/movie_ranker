package com.example

import com.imdb.{MovieDataSchemas, Utils}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class MovieRankerTest extends AnyFunSuite with BeforeAndAfterAll {

  // Initialize spark as a lazy val
  private lazy val spark: SparkSession = SparkSession.builder()
    .appName("IMDb Streaming Application Test")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    super.afterAll()
  }

    test("computeTop10Movies should return the correct top 10 movies") {
      import spark.implicits._

      // Sample data for titleRatings
      val titleRatingsData = Seq(
        Row("tt001", 8.5f, 1500L),
        Row("tt002", 9.0f, 2000L),
        Row("tt003", 7.5f, 800L),
        Row("tt004", 6.0f, 500L),
        Row("tt005", 9.5f, 3000L)
      )

      val titleRatings = spark.createDataFrame(
        spark.sparkContext.parallelize(titleRatingsData),
        MovieDataSchemas.titleRatingsSchema
      )

      // Sample data for titleBasics
      val titleBasicsData = Seq(
        Row("tt001", "Movie One"),
        Row("tt002", "Movie Two"),
        Row("tt003", "Movie Three"),
        Row("tt004", "Movie Four"),
        Row("tt005", "Movie Five")
      )

      val titleBasicsSchema = StructType(Array(
        StructField("tconst", StringType, true),
        StructField("primaryTitle", StringType, true)
      ))

      val titleBasics = spark.createDataFrame(
        spark.sparkContext.parallelize(titleBasicsData),
        titleBasicsSchema
      )

      val avgNumVotes = titleRatings.agg(avg("numVotes")).first().getAs[Double]("avg(numVotes)")


      // Invoke the method under test
      val top10Movies = Utils.calculateTopMovies(titleRatings, titleBasics, 10)

      // Collect results
      val results = top10Movies.collect()

      // Assertions
      assert(results.length == 5, s"Expected 5 movies, but got ${results.length}")

      assert(results(0).getAs[String]("primaryTitle") == "Movie Five", s"Expected 'Movie Five' as the top movie, but got ${results(0).getAs[String]("primaryTitle")}")

      // Assert that the ranking is approximately equal to the calculated value
      val expectedRanking = (3000.0 / avgNumVotes) * 9.5
      val actualRanking = results(0).getAs[Double]("ranking")
      val delta = 1e-4
      assert(math.abs(actualRanking - expectedRanking) < delta, s"Expected ranking ~$expectedRanking, but got $actualRanking")
    }

    test("getTopPersons should return the correct most credited persons") {
      import spark.implicits._

      // Sample data for titlePrincipals
      val titlePrincipalsData = Seq(
        Row("tt001", 1, "nm001"),
        Row("tt001", 2, "nm002"),
        Row("tt002", 1, "nm001"),
        Row("tt003", 1, "nm003"),
        Row("tt004", 1, "nm004"),
        Row("tt005", 1, "nm001"),
        Row("tt005", 2, "nm002")
      )

      val titlePrincipalsSchema = StructType(Array(
        StructField("tconst", StringType, true),
        StructField("ordering", IntegerType, true),
        StructField("nconst", StringType, true)
      ))

      val titlePrincipals = spark.createDataFrame(
        spark.sparkContext.parallelize(titlePrincipalsData),
        titlePrincipalsSchema
      )

      // Sample data for nameBasics
      val nameBasicsData = Seq(
        Row("nm001", "Person One"),
        Row("nm002", "Person Two"),
        Row("nm003", "Person Three"),
        Row("nm004", "Person Four")
      )

      val nameBasicsSchema = StructType(Array(
        StructField("nconst", StringType, true),
        StructField("primaryName", StringType, true)
      ))

      val nameBasics = spark.createDataFrame(
        spark.sparkContext.parallelize(nameBasicsData),
        nameBasicsSchema
      )

      // Invoke the method under test
      val topPersons = Utils.getTopPersons(titlePrincipals, nameBasics, 10)

      // Collect results
      val results = topPersons.collect()

      // Assertions
      assert(results.length == 4, s"Expected 4 top persons, but got ${results.length}")

      // Since the original Python test expects 'Person One' as the top person with 3 movies
      assert(results(0).getAs[String]("primaryName") == "Person One", s"Expected 'Person One' as the top person, but got ${results(0).getAs[String]("primaryName")}")
      assert(results(0).getAs[Long]("movieCount") == 3L, s"Expected 'Person One' to have 3 movies, but got ${results(0).getAs[Long]("movieCount")}")
    }

}
