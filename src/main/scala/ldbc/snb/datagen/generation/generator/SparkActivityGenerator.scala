package ldbc.snb.datagen.generation.generator

import ldbc.snb.datagen.entities.dynamic.Activity
import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.generators.PersonActivityGenerator
import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.{GeneratorConfiguration, Utils}
import ldbc.snb.datagen.{DatagenContext, DatagenParams}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._

object SparkActivityGenerator {
  def apply(persons: RDD[Person], ranker: SparkRanker, conf: GeneratorConfiguration, partitions: Option[Int] = None)(implicit spark: SparkSession): RDD[Activity] = {
    val blockSize = DatagenParams.blockSize
    val blocks = ranker(persons)
      .map { case (k, v) => (k / blockSize, v) }
      .groupByKey()
      .pipeFoldLeft(partitions)((rdd: RDD[(Long, Iterable[Person])], p: Int) => rdd.coalesce(p))

    blocks
      .mapPartitions(groups => {
        DatagenContext.initialize(conf)
        val generator = new PersonActivityGenerator
        for {(blockId, persons) <- groups} yield {
          blockId -> generator.generateActivityForBlock(blockId.toInt, persons.toList.asJava).toArray(Utils.arrayOfSize[Activity])
        }
      })
      .flatMap[Activity] { case (_, v) => v }
  }
}
