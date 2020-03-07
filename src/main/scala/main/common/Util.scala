package main.common

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}


object Util {
  private val selectedSeries = Files.readAllLines(Paths.get("src/main/resources/SelectedSeries.txt"))

  private val sparkConf = new SparkConf().setAppName("SOME APP NAME").setMaster("local[3]").set("spark.executor.memory", "1g")
  val session: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

  private def createReader() = session.read
    .option("inferSchema", "true")
    .option("encoding", "UTF-8")
    .option("header", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("multiLine", value = true).option("ignoreTrailingWhiteSpace", value = true)


  implicit val sc: SparkContext = session.sparkContext
  implicit val seriesEncoder: Encoder[Series] = Encoders.product[Series]
  implicit val countryEncode: Encoder[Country] = Encoders.product[Country]
  implicit val indicatorEncoder: Encoder[Indicator] = Encoders.product[Indicator]
  implicit val lifeExpEncoder: Encoder[LifeExp] = Encoders.product[LifeExp]
  implicit val mapEncoder: Encoder[Map[String, String]] = Encoders.kryo[Map[String, String]]


  def rowFromMap(map: Map[String, String], schema: StructType): Row = {
    val filled = schema.fields.map(_.name)
      .map(map.getOrElse(_, null))

    Row(filled: _*)

  }

  def readCsvAs[T](path: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    val cols = encoder.schema.fields.map(_.name)
    createReader().csv(path).select(cols.head, cols.tail: _*).as[T]

  }

  def writeToCsv(dataFrame: DataFrame, path: String) = {
    dataFrame.repartition(1)
      .write
      .mode(SaveMode.Append)
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .option("header", "true")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("nullValue", null)
      .option("multiLine", value = true).option("ignoreTrailingWhiteSpace", value = true)
      .csv(path)
  }

  def readCsv[T](path: String)(implicit encoder: Encoder[T]): DataFrame = {
    createReader().csv(path)
  }

  def isSelectedSeries(series: Series): Boolean = selectedSeries.contains(series.indicatorName)

}