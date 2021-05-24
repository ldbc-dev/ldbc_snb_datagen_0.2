package ldbc.snb.datagen.transformation

import ldbc.snb.datagen.SparkApp
import ldbc.snb.datagen.spark.DataFrameLike
import ldbc.snb.datagen.transformation.model.{BatchedEntity, Graph, GraphDef, Mode}
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.transformation.io._
import ldbc.snb.datagen.transformation.transform.{ExplodeAttrs, ExplodeEdges, GenRawToRawTransform, RawToBiTransform, RawToInteractiveTransform}
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import shapeless._

object TransformationStage extends SparkApp with Logging {
  override def appName: String = "LDBC SNB Datagen for Spark: TransformationStage"

  case class Args(
    generatedRaw: Graph[Mode.GenRaw.type, DataFrame],
    numThreads: Option[Int],
    outputDir: String = "out",
    explodeEdges: Boolean = false,
    explodeAttrs: Boolean = false,
    simulationStart: Long = 0,
    simulationEnd: Long = 0,
    mode: Mode = Mode.Raw,
    format: String = "csv",
    formatOptions: Map[String, String] = Map.empty
  )

  def run(args: Args)(implicit spark: SparkSession) = {
    object write extends Poly1 {
      implicit def caseSimple[M <: Mode](implicit ev: M#Layout[DataFrame] =:= DataFrame) = at[Graph[M, DataFrame]](g =>
        GraphWriter[M, DataFrame].write(g, args.outputDir, new WriterFormatOptions(args.format, g.mode, args.formatOptions))
      )

      implicit def caseBatched[M <: Mode](implicit ev: M#Layout[DataFrame] =:= BatchedEntity[DataFrame]) = at[Graph[M, DataFrame]](g =>
        GraphWriter[M, DataFrame].write(g, args.outputDir, new WriterFormatOptions(args.format, g.mode, args.formatOptions))
      )
    }

    type OutputTypes = Graph[Mode.Raw.type, DataFrame] :+:
      Graph[Mode.Interactive, DataFrame] :+:
      Graph[Mode.BI, DataFrame] :+:
      CNil

    import DataFrameLike.ops._
    import DataFrameLike.instances._

    args.generatedRaw
      .pipe(GenRawToRawTransform(args.numThreads).transform)
      .pipe(_.persist(StorageLevel.DISK_ONLY))
      .pipeFoldLeft(args.explodeAttrs.fork)((graph, _: Unit) => ExplodeAttrs.transform(graph))
      .pipeFoldLeft(args.explodeEdges.fork)((graph, _: Unit) => ExplodeEdges.transform(graph))
      .pipe[OutputTypes] {
        g =>
          args.mode match {
            case bi@Mode.BI(_, _) => Inr(Inr(Inl(RawToBiTransform(bi, args.simulationStart, args.simulationEnd).transform(g))))
            case interactive@Mode.Interactive(_) => Inr(Inl(RawToInteractiveTransform(interactive, args.simulationStart, args.simulationEnd).transform(g)))
            case Mode.Raw => Inl(g)
            case Mode.GenRaw => throw new UnsupportedOperationException("Cannot serialize GenRaw")
          }
      }
      .pipe(_.map(write))
    ()
  }
}
