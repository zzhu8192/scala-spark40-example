package com.uj.example


import com.uj.testutils.TestSparkSession
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row, SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.junit.{ClassRule, Rule}
import org.junit.rules.TemporaryFolder
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.concurrent.ExecutionContext
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import java.nio.file.Files

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

@RunWith(classOf[JUnitRunner])
class ExampleTest extends AnyFlatSpec with TestSparkSession {

  lazy val testSparkSessions: SparkSession = sparkSession
  import testSparkSessions.implicits._

  private[this] implicit val ec: ExecutionContext = ExecutionContext.global

  @Rule
  val tempFolder = new TemporaryFolder

  def arrangeSeedData() = {
    testSparkSessions.read.options(Example.CSVSparkReadOptions).csv(testSparkSessions.sparkContext.parallelize(
    """id,name
      |1,zheng
      |2,zhu
""".stripMargin.linesIterator.toSeq).toDS())
  }

  behavior of "Test API Transform Function"
  it should "match expected dataframe content " in {
    val seedData = arrangeSeedData()

    //Act
    val resultData = Example.doTransformWithAPI(testSparkSessions, seedData.toDF())

    println(resultData.schema)
    // Assert
    val actualDataArray = resultData.collect()
    actualDataArray.length shouldBe 2
    resultData.schema.fields.length shouldBe 3

    resultData.schema.fields(0).name shouldBe "id"
    resultData.schema.fields(1).name shouldBe "name"
    resultData.schema.fields(2).name shouldBe "len"

    actualDataArray(0).get(0) shouldBe "1"
    actualDataArray(0).get(1) shouldBe "zheng"
    actualDataArray(0).get(2) shouldBe 5
  }

  behavior of "Test SQL Transform Function"
  it should "match expected dataframe content " in {
    val seedData = arrangeSeedData()

    //Act
    val resultData = Example.doTransformWithSQL(testSparkSessions, seedData.toDF())

    println(resultData.schema)
    // Assert
    val actualDataArray = resultData.collect()
    actualDataArray.length shouldBe 2
    resultData.schema.fields.length shouldBe 3

    resultData.schema.fields(0).name shouldBe "id"
    resultData.schema.fields(1).name shouldBe "name"
    resultData.schema.fields(2).name shouldBe "len"

    actualDataArray(0).get(0) shouldBe "1"
    actualDataArray(0).get(1) shouldBe "zheng"
    actualDataArray(0).get(2) shouldBe 5
  }


  behavior of "test range function with map"
  it should "match expected dataframe content " in {
    val seedData =    testSparkSessions.read.options(Example.CSVSparkReadOptions).csv(testSparkSessions.sparkContext.parallelize(
      """start,n
        |2,4
        |3,4
""".stripMargin.linesIterator.toSeq).toDS())

    //Act
    val resultData = Example.doRangeTransform(testSparkSessions, seedData.toDF())

    resultData.show()
    // Assert
    val actualDataArray = resultData.collect()
    actualDataArray.length shouldBe 2
    resultData.schema.fields.length shouldBe 3

    resultData.schema.fields(0).name shouldBe "start"
    resultData.schema.fields(1).name shouldBe "count"
    resultData.schema.fields(2).name shouldBe "range"

    actualDataArray(0).get(0) shouldBe 2
    actualDataArray(0).get(1) shouldBe 4
    actualDataArray(0).get(2) shouldBe Array(2,4,6,8)
  }


  behavior of "test range function with UDF"
  it should "match expected dataframe content " in {
    val seedData =    testSparkSessions.read.options(Example.CSVSparkReadOptions).csv(testSparkSessions.sparkContext.parallelize(
      """start,n
        |2,4
        |3,4
""".stripMargin.linesIterator.toSeq).toDS())

    //Act
    val resultData = Example.doRangeTransformWithUDF(testSparkSessions, seedData.toDF())

    resultData.show()
    // Assert
    val actualDataArray = resultData.collect()
    actualDataArray.length shouldBe 2
    resultData.schema.fields.length shouldBe 3

    resultData.schema.fields(0).name shouldBe "start"
    resultData.schema.fields(1).name shouldBe "n"
    resultData.schema.fields(2).name shouldBe "range"

    actualDataArray(0).get(0) shouldBe "2"
    actualDataArray(0).get(1) shouldBe "4"
    actualDataArray(0).get(2) shouldBe Array(2,4,6,8)
  }


  behavior of "test hive table creation from df"
  it should "select from hive table exists " in {
    // Arrange
    val seedData =    testSparkSessions.read.options(Example.CSVSparkReadOptions).csv(testSparkSessions.sparkContext.parallelize(
      """id,test
        |1,zz
        |2,yy
""".stripMargin.linesIterator.toSeq).toDS())
    val filePath = Files.createTempDirectory("tmpTestParquet").toFile.getAbsolutePath.replace('\\', '/')

    //Act
    Example.createHiveTableFromDf(testSparkSessions, seedData, filePath, "test_db", "test_table")
    val resultData=testSparkSessions.sql("select * from test_db.test_table order by id asc")
    // Assert
    val actualDataArray = resultData.collect()
    actualDataArray.length shouldBe 2
    resultData.schema.fields.length shouldBe 2

    actualDataArray(0).get(0) shouldBe "1"
    actualDataArray(0).get(1) shouldBe "zz"
  }
  /*
   val schema = StructType(
     StructField("id", StringType, nullable = false) ::
     StructField("name", StringType, nullable = false) :: Nil)
   */

}
