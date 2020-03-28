package main.common

case class Series(seriesCode: String, topic: String, indicatorName: String, aggregationMethod: Option[String])

case class Indicator(countryCode: String, indicatorCode: String, year: Int, value: Double)


case class Country(countryCode: String, shortName: String, incomeGroup: String, region: String)


case class LifeExp(country: String, year: Int, lifeExpectancy: Double, gdp: Double)
