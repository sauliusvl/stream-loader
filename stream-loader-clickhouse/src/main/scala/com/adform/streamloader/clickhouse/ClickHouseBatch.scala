package com.adform.streamloader.clickhouse

/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

import com.adform.streamloader.clickhouse.rowbinary.{
  RowBinaryClickHousePrimitiveTypeWriter,
  RowBinaryClickHouseRecordEncoder
}
import com.adform.streamloader.model._
import com.adform.streamloader.sink.batch.Compression
import com.adform.streamloader.sink.batch.format.FormattedRecordBatch
import com.adform.streamloader.sink.batch.storage.InDataOffsetBatchStorage
import com.adform.streamloader.sink.batch.stream.{BaseStreamBatchBuilder, CsvStreamBatchBuilder, StreamBatch}
import com.adform.streamloader.sink.encoding.csv.CsvRecordEncoder
import Compression.NONE
import com.adform.streamloader.util.Logging
import com.clickhouse.data.{ClickHouseCompression, ClickHouseFormat, ClickHousePassThruStream}
import com.clickhouse.jdbc.ClickHouseConnection
import org.apache.kafka.common.TopicPartition

import java.io.InputStream
import java.sql.Connection
import javax.sql.DataSource
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Using

case class ClickHouseBatch(
    inputStream: InputStream,
    format: ClickHouseFormat,
    compression: ClickHouseCompression
)

object ClickHouseBatch {
  def toClickHouseCompression(compression: Compression): ClickHouseCompression = compression match {
    case Compression.NONE => ClickHouseCompression.NONE
    case Compression.ZSTD => ClickHouseCompression.ZSTD
    case Compression.GZIP => ClickHouseCompression.GZIP
    case Compression.BZIP => ClickHouseCompression.BZ2
    case Compression.LZ4 => ClickHouseCompression.LZ4
    case _ => throw new UnsupportedOperationException(s"Compression $compression is not supported by ClickHouse")
  }
}

class ClickHouseCsvBatchBuilder[R: CsvRecordEncoder](stream: StreamBatch[InputStream], compression: Compression = NONE)
    extends CsvStreamBatchBuilder[R, ClickHouseBatch](
      stream.map(i => ClickHouseBatch(i, ClickHouseFormat.CSV, ClickHouseBatch.toClickHouseCompression(compression)))
    )

class ClickHouseRowBinaryBatchBuilder[R: RowBinaryClickHouseRecordEncoder](
    stream: StreamBatch[InputStream],
    compression: Compression = NONE
) extends BaseStreamBatchBuilder[R, ClickHouseBatch](
      stream.map(i =>
        ClickHouseBatch(i, ClickHouseFormat.RowBinary, ClickHouseBatch.toClickHouseCompression(compression))
      ),
      compression,
      blockSize = 1
    ) {

  private val pw = new RowBinaryClickHousePrimitiveTypeWriter {
    override def writeByte(b: Int): Unit = {
      out.write(b)
    }
  }
  private val recordEncoder = implicitly[RowBinaryClickHouseRecordEncoder[R]]

  override def add(record: R): Int = {
    recordEncoder.write(record, pw)
    super.add(record)
  }
}

class ClickHouseRecordBatchStorage(
    dbDataSource: DataSource,
    table: String,
    topicColumnName: String,
    partitionColumnName: String,
    offsetColumnName: String,
    watermarkColumnName: String
) extends InDataOffsetBatchStorage[FormattedRecordBatch[Unit, ClickHouseBatch]]
    with Logging {

  def committedPositions(connection: Connection): Map[TopicPartition, StreamPosition] = {
    val positionQuery =
      s"""SELECT
         |  $topicColumnName,
         |  $partitionColumnName,
         |  MAX($offsetColumnName) + 1,
         |  MAX($watermarkColumnName)
         |FROM $table
         |WHERE isNotNull($topicColumnName) AND isNotNull($partitionColumnName)
         |GROUP BY $topicColumnName, $partitionColumnName
         |""".stripMargin

    Using.resource(connection.prepareStatement(positionQuery)) { statement =>
      {
        log.info(s"Running stream position query: $positionQuery")
        Using.resource(statement.executeQuery()) { result =>
          val positions: mutable.HashMap[TopicPartition, StreamPosition] = mutable.HashMap.empty
          while (result.next()) {
            val topic = result.getString(1)
            val partition = result.getInt(2)
            val offset = result.getLong(3)
            val watermark = Timestamp(result.getTimestamp(4).getTime)
            if (!result.wasNull()) {
              val topicPartition = new TopicPartition(topic, partition)
              val position = StreamPosition(offset, watermark)
              positions.put(topicPartition, position)
            }
          }
          positions.toMap
        }
      }
    }
  }

  override def committedPositions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    Using.resource(dbDataSource.getConnection()) { connection =>
      val positions = committedPositions(connection)
      topicPartitions.map(tp => (tp, positions.get(tp))).toMap
    }
  }

  override def commitBatchWithOffsets(recordBatch: FormattedRecordBatch[Unit, ClickHouseBatch]): Unit = {
    val batch = recordBatch.partitionBatches(())
    Using.resource(dbDataSource.getConnection) { connection =>
      Using.resource(connection.unwrap(classOf[ClickHouseConnection]).createStatement) { statement =>
        statement
          .write()
          .data(ClickHousePassThruStream.of(batch.inputStream, batch.compression, batch.format))
          .table(table)
          .params(Map("max_insert_block_size" -> recordBatch.recordCount.toString).asJava) // atomic insert
          .executeAndWait()
      }
    }
  }
}

object ClickHouseRecordBatchStorage {

  case class Builder(
      private val _dbDataSource: DataSource,
      private val _table: String,
      private val _topicColumnName: String,
      private val _partitionColumnName: String,
      private val _offsetColumnName: String,
      private val _watermarkColumnName: String
  ) {

    /**
      * Sets a data source for ClickHouse JDBC connections.
      */
    def dbDataSource(source: DataSource): Builder = copy(_dbDataSource = source)

    /**
      * Sets the table to load data to.
      */
    def table(name: String): Builder = copy(_table = name)

    /**
      * Sets the names of the columns in the table that are used for storing the stream position
      * this row was producer from. Used in the initialization query that determines committed stream positions.
      */
    def rowOffsetColumnNames(
        topicColumnName: String = "_topic",
        partitionColumnName: String = "_partition",
        offsetColumnName: String = "_offset",
        watermarkColumnName: String = "_watermark"
    ): Builder =
      copy(
        _topicColumnName = topicColumnName,
        _partitionColumnName = partitionColumnName,
        _offsetColumnName = offsetColumnName,
        _watermarkColumnName = watermarkColumnName
      )

    def build(): ClickHouseRecordBatchStorage = {
      if (_dbDataSource == null) throw new IllegalStateException("Must provide a ClickHouse data source")
      if (_table == null) throw new IllegalStateException("Must provide a valid table name")

      new ClickHouseRecordBatchStorage(
        _dbDataSource,
        _table,
        _topicColumnName,
        _partitionColumnName,
        _offsetColumnName,
        _watermarkColumnName
      )
    }
  }

  def builder(): Builder = Builder(null, null, "_topic", "_partition", "_offset", "_watermark")
}
