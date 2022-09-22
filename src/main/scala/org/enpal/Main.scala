package org.enpal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, min, udf}
import org.apache.spark.storage.StorageLevel

import math.abs
import scala.util.Random

object Main {

  case class CoordinateCustomer(longitudeCustomer: Double, latitudeCustomer: Double)

  private val customerDataUrl = "data/customerData.json"
  private val basesDataUrl = "data/basesData.json"
  private val teamDataUrl = "data/solarTeamData.json"

  // Geo Box Germany, source: https://gist.github.com/graydon/11198540
  private val germanyMinLat = 5.98865807458
  private val germanyMaxLat = 15.0169958839
  private val germanyMinLon = 47.3024876979
  private val germanyMaxLon = 54.983104153

  private val AVERAGE_RADIUS_OF_EARTH_KM = 6371

  val convertToInstallRateUDF: UserDefinedFunction = udf((skillLevel: Int) => {
    // check whether level is in set(1,2,3)
    skillLevel match {
      case 1 => 100 // 100 is not unlimited, but it is a lot...?
      case 2 => 22
      case 3 => Random.between(0, 22) // instead of estimation of installRate by...?
    }
  })

  val assignRandomCoordinatesUDF: UserDefinedFunction = udf((lonCustomer: Double, latCustomer: Double) =>
    if ((germanyMinLon <= lonCustomer && lonCustomer <= germanyMaxLon)
      && (germanyMinLat <= latCustomer && latCustomer <= germanyMaxLat)) {
      CoordinateCustomer(lonCustomer, latCustomer)
    } else {
      CoordinateCustomer(Random.between(germanyMinLon, germanyMaxLon), Random.between(germanyMinLat, germanyMaxLat))
    }
  )

  val calculateDistanceUDF: UserDefinedFunction = udf((lonTarget: Int, latTarget: Int, lonOrigin: Int, latOrigin: Int) => {
    val latDistance = Math.toRadians(abs(latTarget - latOrigin))
    val lngDistance = Math.toRadians(abs(lonTarget - lonOrigin))
    val sinLat = Math.sin(latDistance / 2)
    val sinLng = Math.sin(lngDistance / 2)
    val a = sinLat * sinLat +
      (Math.cos(Math.toRadians(latTarget)) *
        Math.cos(Math.toRadians(latOrigin)) *
        sinLng * sinLng)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    (AVERAGE_RADIUS_OF_EARTH_KM * c).toInt
  })

  def main(args: Array[String]): Unit = {

    println("Hello Enpal!")

    val spark = SparkSession
      .builder()
      .appName("enpal-assesment")
      .master("local[*]")
      .getOrCreate()

    val dfTeams = spark.read.json(teamDataUrl)
    val dfBases = spark.read.json(basesDataUrl)
    val dfCustomers = spark.read.json(customerDataUrl)
      .withColumnRenamed("availabilityFrom", "availabilityFromCustomer")

    /** Assign Coordinates UDF if not provided */
    val dfCustomersCoordinates = dfCustomers.withColumn("coordinates", assignRandomCoordinatesUDF(dfCustomers("longitudeCustomer"), dfCustomers("latitudeCustomer")))
      .select("customerId","panelsToInstall", "availabilityFromCustomer", "coordinates.*" )
      .persist(StorageLevel.MEMORY_ONLY) // Random values change otherwise everytime an action is performed on the dataframe

    /** Convert installRate and join bases and teams together */
    val dfTeamsInstallRate = dfTeams.withColumn("installRate", convertToInstallRateUDF(dfTeams.col("skillLevel")))
    val dfTeamsBased = dfTeamsInstallRate.join(dfBases, dfTeamsInstallRate("basedAt") === dfBases("cityId"))

    /**Join and Filter out availabilities between teams and customers */
    val dfBig = dfCustomersCoordinates
      .join(dfTeamsBased.withColumnRenamed("latitude", "latitudeTeam").withColumnRenamed("longitude", "longitudeTeam"),
        dfCustomersCoordinates("availabilityFromCustomer") >= dfTeamsBased("availabilityFrom") && dfCustomersCoordinates("availabilityFromCustomer") <= dfTeamsBased("availabilityTo"))
      .where(col("panelsToInstall" ) <= col("installRate"))

    /** Calculate distance in kilometers using advanced coordinate conversion */
    val dfBigWithDistance = dfBig
      .withColumn("distance", calculateDistanceUDF(dfBig("latitudeCustomer"), dfBig("longitudeCustomer"), dfBig("latitudeTeam"), dfBig("longitudeTeam")))

    /** Group by customers and find best team with minimum distance */
    val dfGroupedReduced = dfBigWithDistance
      .groupBy("customerId").agg(min("distance"))
      .withColumnRenamed("min(distance)", "distance")

    val columnsResult = Seq("customerId", "distance")
    val dfJoined = dfBigWithDistance.join(dfGroupedReduced, columnsResult)
      .select("teamId", "customerId", "distance", "cityName")
      .sort("teamId")


    /** Collect dataframe into array and print out in nice formatting */
    dfJoined.collect().foreach(row => println("team " + row.get(0).toString + " => customer " + row.get(1).toString + ", distance to travel: " + row.get(2).toString + " km from " + row.get(3).toString))

    /** Finish */
    spark.close()
  }
}

