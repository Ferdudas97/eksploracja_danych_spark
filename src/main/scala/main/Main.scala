package main

import main.common.Util._
import main.common.Util.session.implicits._
import main.common.{Country, Indicator, LifeExp, Series}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object Main {

  def main(args: Array[String]): Unit = {

    val selectedSeries = selectSeries()
    val cleanedData = cleanData(selectedSeries)
    val seriesCodes = selectedSeries.map(_.seriesCode)
    val schema = StructType(seriesCodes.map(StructField(_, StringType))).add("year", StringType).add("countryCode", StringType)
    val weights = makeWeights(cleanedData, schema)
    val labels = createLabels()
    val analyzableData = weights.join(labels, Seq("countryCode", "year"))
    writeToCsv(analyzableData, "src/main/resources/out/Test.csv")

  }

  private def selectSeries(): Array[Series] = {
    val series = readCsvAs[Series]("src/main/resources/Series.csv")
    val selectedSeries = series.filter(isSelectedSeries(_)).collect()
    selectedSeries
  }

  private def cleanData(selectedSeries: Array[Series]): Dataset[Indicator] = {

    val indicators = readCsvAs[Indicator]("src/main/resources/Indicators.csv")
    val selectedIndicators = indicators.filter(isSelectedIndicator(selectedSeries, _))
      .filter(it => isInAnalyzedYears(it.year))

    selectedIndicators
  }

  private def makeWeights(indicators: Dataset[Indicator], schema: StructType) = {
    indicators.groupByKey(it => (it.countryCode, it.year))
      .mapGroups { case (it, ind) =>
        val weights = ind.map(it => it.indicatorCode -> it.value.toString).toMap

        val map = weights + ("countryCode" -> it._1, "year" -> it._2.toString)
        rowFromMap(map, schema)
      }(RowEncoder(schema))
  }

  private def makeAnalyzableData2(indicators: Dataset[Indicator]) = indicators.rdd.groupBy(it => (it.countryCode, it.year))
    .map { case (it, ind) =>
      val weights = ind.map(it => it.indicatorCode -> it.value.toString).toList
      val row = ("country" -> it._1) :: ("year" -> it._2.toString) :: weights
      row
    }


  case class Weights(countryCode: String, values: Iterable[Indicator])

  private def createLabels() = {
    val led = readCsvAs[LifeExp]("src/main/resources/LifeExp.csv")
    val countries = readCsvAs[Country]("src/main/resources/Country.csv").withColumnRenamed("shortName", "country")
    val labels = led.join(countries, "country")
    labels
  }

  private def isInAnalyzedYears(year: Int) = 2000 <= year && year <= 2015

  private def isSelectedIndicator(series: Seq[Series], indicator: Indicator) = series.map(_.seriesCode)
    .contains(indicator.indicatorCode)

}