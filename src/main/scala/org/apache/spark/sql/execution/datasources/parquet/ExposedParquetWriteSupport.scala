package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.OutputCommitter
import org.apache.parquet.hadoop.{ParquetOutputCommitter, ParquetOutputFormat}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.io.api.RecordConsumer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

class ExposedParquetWriteSupport(
                                  dataSchema: StructType
                                ) extends WriteSupport[InternalRow] {
  val inner = new ParquetWriteSupport

  override def init(conf: Configuration): WriteSupport.WriteContext = {

    val committerClass =
      conf.getClass(
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key,
        classOf[ParquetOutputCommitter],
        classOf[OutputCommitter])

    conf.setClass(
      SQLConf.OUTPUT_COMMITTER_CLASS.key,
      committerClass,
      classOf[OutputCommitter])

    ParquetWriteSupport.setSchema(dataSchema, conf)

    conf.set(
      SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key,
      //sparkSession.sessionState.conf.writeLegacyParquetFormat.toString
      "false")

    conf.set(
      SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key,
      //sparkSession.sessionState.conf.parquetOutputTimestampType.toString
      "INT96")

    inner.init(conf)
  }

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = inner.prepareForWrite(recordConsumer)

  override def write(record: InternalRow): Unit = inner.write(record)
}
