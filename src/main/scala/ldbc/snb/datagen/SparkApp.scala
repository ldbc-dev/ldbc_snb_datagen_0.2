package ldbc.snb.datagen

import ldbc.snb.datagen.syntax._
import org.apache.spark.sql.SparkSession

trait SparkApp {
  def appName: String

  implicit def spark = SparkSession
    .builder()
    .appName(appName)
    .pipe(applySparkConf(defaultSparkConf))
    .getOrCreate()

  private def applySparkConf(sparkConf: Map[String, String])(builder: SparkSession.Builder) =
    sparkConf.foldLeft(builder) { case (b, (k, v)) => b.config(k, v) }

  def defaultSparkConf: Map[String, String] = Map(
    "spark.sql.session.timeZone" -> "GMT"
  )

}
