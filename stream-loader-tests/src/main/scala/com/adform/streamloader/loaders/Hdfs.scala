/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.loaders

import com.adform.streamloader.hadoop.{DerivedAvroParquetFileBuilder, HadoopFileStorage}
import com.adform.streamloader.model.{ExampleMessage, StreamRecord, Timestamp}
import com.adform.streamloader.sink.batch.v2.{BatchCommitStrategy, RecordBatchingSink}
import com.adform.streamloader.sink.batch.v2.formatting.{FormattedRecordBatch, FormattingRecordBatcher}
import com.adform.streamloader.sink.file.FileCommitStrategy.ReachedAnyOf
import com.adform.streamloader.sink.file._
import com.adform.streamloader.source.KafkaSource
import com.adform.streamloader.util.ConfigExtensions._
import com.adform.streamloader.{Loader, StreamLoader}
import com.sksamuel.avro4s.ScalePrecision
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.util.HadoopOutputFile

import java.time.LocalDate
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.math.BigDecimal.RoundingMode.RoundingMode

object TestParquetHdfsLoader extends Loader {

  def main(args: Array[String]): Unit = {

    val cfg = ConfigFactory.load().getConfig("stream-loader")

    val hadoopConf = new Configuration()
    hadoopConf.set("fs.hdfs.impl.disable.cache", "true")

    cfg.getConfigOpt("hadoop").foreach { hc =>
      hc.entrySet().asScala.foreach { e =>
        hadoopConf.set(e.getKey, e.getValue.unwrapped().toString)
      }
    }

    val hadoopFileSystem = FileSystem.get(hadoopConf)

    val source = KafkaSource
      .builder()
      .consumerProperties(cfg.getConfig("kafka.consumer").toProperties)
      .pollTimeout(cfg.getDuration("kafka.poll-timeout"))
      .topics(Seq(cfg.getString("kafka.topic")))
      .build()

    implicit val scalePrecision: ScalePrecision = ExampleMessage.SCALE_PRECISION
    implicit val roundingMode: RoundingMode = ExampleMessage.ROUNDING_MODE

    val sink = RecordBatchingSink
      .builder()
      .recordBatcher(
        FormattingRecordBatcher
          .builder()
          .formatter((r: StreamRecord) => Seq(ExampleMessage.parseFrom(r.consumerRecord.value())))
          .partitioner((r, _) => Timestamp(r.consumerRecord.timestamp()).toDate)
          .batchBuilder(_ =>
            new DerivedAvroParquetFileBuilder[ExampleMessage](
              HadoopOutputFile.fromPath(
                new Path(cfg.getString("hdfs.staging-directory"), UUID.randomUUID() + ".parquet"),
                hadoopFileSystem.getConf
              )
            )
          )
          .build()
      )
      .batchCommitStrategy(
        BatchCommitStrategy.ReachedAnyOf(recordsWritten = Some(cfg.getLong("file.max.records")))
      )
      .batchStorage(
        HadoopFileStorage
          .builder()
          .hadoopFS(hadoopFileSystem)
          .stagingBasePath(cfg.getString("hdfs.staging-directory"))
          .destinationBasePath(cfg.getString("hdfs.base-directory"))
          .destinationFilePathFormatter(
            new TimePartitioningFilePathFormatter[LocalDate](
              cfg.getStringOpt("file.time-partition.pattern"),
              None
            )
          )
          .build()
      )
      .build()

    val loader = new StreamLoader(source, sink)

    sys.addShutdownHook {
      loader.stop()
      hadoopFileSystem.close()
    }

    loader.start()
  }
}
