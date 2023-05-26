import org.apache.spark.sql.functions.column
import org.apache.spark.sql.SparkSession

object AppMain {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder()
      .appName("Spark2.3-1")
      .master("local")
      .getOrCreate()

    val bikeSharingDF = sparkSession.read
      .option("inferSchema", "true")
      .option("sep", ",")
      .option("header", "true")
      .csv("src/main/resources/bike_sharing.csv")

    bikeSharingDF.printSchema()

    val selectFrame = bikeSharingDF.select(
      column("Hour"),
      column("TEMPERATURE"),
      column("HUMIDITY"),
      column("WIND_SPEED")
    )

    selectFrame.show(3)
  }
}
