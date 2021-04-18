package ldbc.snb.datagen.transformation.io

import ldbc.snb.datagen.transformation.model.{Batched, BatchedEntity, EntityType, Graph, GraphLike, Mode}
import ldbc.snb.datagen.transformation.model.EntityType.{Attr, Edge, Node}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SaveMode, SparkSession}
import shapeless.{Generic, Poly1}
import better.files._
import ldbc.snb.datagen.transformation.model.Mode.Raw
import ldbc.snb.datagen.util.{Logging, SparkUI}

import scala.collection.immutable.TreeMap

trait GraphWriter[M <: Mode] {
  type Data
  def write(graph: Graph[M, Data], path: String, options: WriterFormatOptions): Unit
}

object GraphWriter {
  type Aux[M <: Mode, D] = GraphWriter[M] { type Data = D }
  def apply[M <: Mode, D](implicit ev: GraphWriter.Aux[M, D]): GraphWriter.Aux[M, D] = ev
}

class WriterFormatOptions(val format: String, mode: Mode, private val customFormatOptions: Map[String, String] = Map.empty) {
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

object Writer {
  object implicits {

    // Heuristic for ordering entity types so that those derived
    // similarly are near each other.
    implicit val cacheFriendlyOrdering = new Ordering[EntityType] {
      private val and = (a: Int, b: Int) => if (a == 0) b else a

      private object mapper extends Poly1 {
        implicit val atNode = at[Node](n => Seq(n.name.hashCode, 0, n.hashCode()))
        implicit val atEdge = at[Edge](e => {
          val primary = e match {
            case Edge("Likes", "Person", "Comment", _, _) => "Comment"
            case Edge("Likes", "Person", "Post", _, _) => "Post"
            case Edge("ContainerOf", "Forum", "Post", _, _) => "Post"
            case Edge(_, source, _, _, _) => source
          }
          Seq(primary.hashCode, 2, e.hashCode)
        })
        implicit val atAttr = at[Attr](a => Seq(a.parent.hashCode, 1, a.hashCode()))
      }

      private def orderingKeys(t: EntityType): Seq[Int] = Generic[EntityType].to(t).map(mapper).unify

      override def compare(x: EntityType, y: EntityType): Int =
        orderingKeys(x).zip(orderingKeys(y)).foldLeft(0)((a, v) => and(a, Ordering[Int].compare(v._1, v._2)))
    }
  }

  def apply[T](dfw: DataFrameWriter[T], writerOptions: WriterFormatOptions) = {
    dfw
      .format("csv")
      .options(writerOptions.formatOptions)
  }
}

private final class DataFrameGraphWriter[M <: Mode](implicit
  ev: M#Layout[DataFrame] =:= DataFrame
) extends GraphWriter[M] with Logging {
  import Writer.implicits._
  type Data = DataFrame

  override def write(graph: Graph[M, DataFrame], path: String, options: WriterFormatOptions): Unit = {
    TreeMap(graph.entities.toSeq: _*).foreach {
      case (tpe, dataset) =>
        SparkUI.job(getClass.getSimpleName, s"write $tpe") {
          log.info(s"$tpe: Writing started")
          Writer.apply(dataset.write, options)
            .mode(SaveMode.Ignore)
            .save((path / options.format / PathComponent[GraphLike[M]].path(graph) / tpe.entityPath).toString())
          log.info(s"$tpe: Writing completed")
        } (dataset.sparkSession)
    }
  }
}

private final class BatchedDataFrameGraphWriter[M <: Mode](implicit
  ev: M#Layout[DataFrame] =:= BatchedEntity[DataFrame]
) extends GraphWriter[M] with Logging {
  import Writer.implicits._
  type Data = DataFrame

  override def write(graph: Graph[M, DataFrame], path: String, options: WriterFormatOptions): Unit = {
    TreeMap(graph.entities.mapValues(ev).toSeq: _*).foreach {
      case (tpe, BatchedEntity(snapshot, insertBatches, deleteBatches)) =>
        SparkUI.job(getClass.getSimpleName, s"write $tpe snapshot") {
          log.info(s"$tpe: Writing snapshot")

          Writer.apply(snapshot.write, options)
            .mode(SaveMode.Ignore)
            .save((path / options.format / PathComponent[GraphLike[M]].path(graph) / "initial_snapshot" / tpe.entityPath).toString())

          log.info(s"$tpe: Writing snapshot completed")
        } (snapshot.sparkSession)

        for { (operation, batches) <- Map("inserts" -> insertBatches, "deletes" -> deleteBatches) } {
          batches.foreach {
            case Batched(entity, partitionKeys) =>
              SparkUI.job(getClass.getSimpleName, s"write $tpe $operation") {
                log.info(f"$tpe: Writing $operation")
                Writer.apply(entity.write, options)
                  .partitionBy(partitionKeys: _*)
                  .mode(SaveMode.Ignore)
                  .save((path / options.format / PathComponent[GraphLike[M]].path(graph) / operation / tpe.entityPath).toString())
                log.info(f"$tpe: Writing $operation completed")
              }(entity.sparkSession)
          }
        }
    }
  }
}

trait GraphWriterInstances {
  implicit def dataFrameGraphWriter[M <: Mode]
  (implicit ev: M#Layout[DataFrame] =:= DataFrame): GraphWriter.Aux[M, DataFrame] = new DataFrameGraphWriter[M]
  implicit def batchedDataFrameGraphWriter[M <: Mode]
  (implicit ev: M#Layout[DataFrame] =:= BatchedEntity[DataFrame]): GraphWriter.Aux[M, DataFrame] = new BatchedDataFrameGraphWriter[M]
}

