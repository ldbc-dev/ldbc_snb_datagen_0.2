package ldbc.snb.datagen.transformation.io

import ldbc.snb.datagen.transformation.model.{Graph, GraphDef, GraphLike, Id, Mode}
import org.apache.spark.sql.{DataFrame, SparkSession}
import better.files._
import ldbc.snb.datagen.syntax.fluentSyntaxOps
import ldbc.snb.datagen.transformation.model.Mode.Raw
import ldbc.snb.datagen.util.Logging
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

trait GraphReader[M <: Mode] {
  type Data
  def read(graphDef: GraphDef[M], path: String, format: String, options: ReaderFormatOptions): Graph[M, Data]
  def exists(graphDef: GraphDef[M], path: String): Boolean
}

object GraphReader {
  type Aux[M <: Mode, D] = GraphReader[M] { type Data = D }

  def apply[M <: Mode, D](implicit ev: GraphReader.Aux[M, D]): GraphReader.Aux[M, D] = ev
}

class ReaderFormatOptions(val format: String, mode: Mode, private val customFormatOptions: Map[String, String] = Map.empty) {
  val defaultCsvFormatOptions = Map(
    "header" -> "true",
    "sep" ->  "|"
  )

  val forcedRawCsvFormatOptions = Map(
    "dateFormat" -> Raw.datePattern,
    "timestampFormat" -> Raw.dateTimePattern
  )

  val formatOptions: Map[String, String] = (format, mode) match {
    case ("csv", Raw) => defaultCsvFormatOptions ++ customFormatOptions ++ forcedRawCsvFormatOptions
    case ("csv", _) => defaultCsvFormatOptions ++ customFormatOptions
    case _ => customFormatOptions
  }
}

object Reader {
  def apply(readerOptions: ReaderFormatOptions)(implicit spark: SparkSession) = {
    spark.read.format(readerOptions.format).options(readerOptions.formatOptions)
  }
}

private final class DataFrameGraphReader[M <: Mode](implicit spark: SparkSession, ev: Id[DataFrame] =:= M#Layout[DataFrame])
  extends GraphReader[M]
    with Logging {
  type Data = DataFrame

  override def read(definition: GraphDef[M], path: String, format: String, options: ReaderFormatOptions): Graph[M, DataFrame] = {
    val entities = for { (entity, schema) <- definition.entities } yield {
      val df = Reader(options)
        .pipeFoldLeft(schema)(_.schema(_))
        .load((path / format / PathComponent[GraphLike[M]].path(definition) / entity.entityPath).toString())
      entity -> ev(df)
    }
    Graph[M, DataFrame](definition.isAttrExploded, definition.isEdgesExploded, definition.mode, entities)
  }

  override def exists(graphDef: GraphDef[M], path: String): Boolean = {
    val hadoopPath = new Path(path)
    FileSystem.get(new URI(path), spark.sparkContext.hadoopConfiguration).exists(hadoopPath)
  }
}

trait GraphReaderInstances {
  implicit def dataFrameGraphReader[M <: Mode]
  (implicit spark: SparkSession, ev: Id[DataFrame] =:= M#Layout[DataFrame]): GraphReader.Aux[M, DataFrame] =
    new DataFrameGraphReader[M]
}
