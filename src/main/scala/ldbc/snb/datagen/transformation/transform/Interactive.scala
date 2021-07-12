package ldbc.snb.datagen.transformation.transform

import ldbc.snb.datagen.sql._
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.model.Cardinality.NN
import ldbc.snb.datagen.transformation.model.EntityType
import ldbc.snb.datagen.transformation.model.EntityType.Edge
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

private object Interactive {

  def columns(tpe: EntityType, cols: Seq[String]) = tpe match {
    case tpe if tpe.isStatic => cols
    case Edge("Knows", "Person", "Person", NN, false) =>
      val rawCols = Set("deletionDate", "explicitlyDeleted", "weight")
      cols.filter(!rawCols.contains(_))
    case _ =>
      val rawCols = Set("deletionDate", "explicitlyDeleted")
      cols.filter(!rawCols.contains(_))
  }

  def calculateBulkLoadThreshold(bulkLoadPortion: Double, simulationStart: Long, simulationEnd: Long) = {
    (simulationEnd - ((simulationEnd - simulationStart) * (1 - bulkLoadPortion)).toLong)
  }

  def snapshotPart(tpe: EntityType, df: DataFrame, bulkLoadThreshold: Long, filterDeletion: Boolean) = {
    val filterBulkLoad = (ds: DataFrame) => ds
      .filter(
        $"creationDate" < to_timestamp(lit(bulkLoadThreshold / 1000)) &&
          (!lit(filterDeletion) || $"deletionDate" >= to_timestamp(lit(bulkLoadThreshold / 1000)))
      )

    tpe match {
      case tpe if tpe.isStatic => df
      case tpe => filterBulkLoad(df).select(columns(tpe, df.columns).map(name => col(qualified(name))): _*)
    }
  }
}
