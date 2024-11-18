/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.iceberg.v2

import com.adform.streamloader.model.{StreamPosition, Timestamp}
import com.adform.streamloader.sink.batch.v2.FormattedRecordBatch
import com.adform.streamloader.sink.batch.v2.storage.InDataOffsetBatchStorage
import org.apache.iceberg.{PartitionKey, Table}
import org.apache.kafka.common.TopicPartition

import java.util.concurrent.locks.Lock

class IcebergRecordBatchStorage(table: Table, commitLock: Option[Lock])
    extends InDataOffsetBatchStorage[FormattedRecordBatch[PartitionKey, IcebergBatch]] {

  private def offsetKey(topic: String, partition: Int): String = {
    s"__consumer_offset:${kafkaContext.consumerGroup}:$topic:$partition"
  }

  override def recover(topicPartitions: Set[TopicPartition]): Unit = {}

  override def commitBatchWithOffsets(batch: FormattedRecordBatch[PartitionKey, IcebergBatch]): Unit =
    commitLock match {
      case Some(lock) =>
        try {
          log.debug("Acquiring Iceberg commit lock")
          lock.lock()
          commitLocked(batch)
        } finally {
          lock.unlock()
          log.debug("Released Iceberg commit lock")
        }

      case None =>
        commitLocked(batch)
    }

  private def commitLocked(batch: FormattedRecordBatch[PartitionKey, IcebergBatch]): Unit = {
    log.debug(s"Starting new Iceberg transaction for ranges ${batch.recordRanges.mkString(",")}")
    val transaction = table.newTransaction()

    batch.partitionBatches.values.foreach(b => {
      b.dataWriteResult
        .dataFiles()
        .forEach(f => {
          transaction.newAppend().appendFile(f).commit()
        })
    })

    batch.recordRanges.foreach(range => {
      transaction
        .updateProperties()
        .set(offsetKey(range.topic, range.partition), s"${range.end.offset}:${range.end.watermark.millis}")
        .commit()
    })

    transaction.commitTransaction()
    log.info(s"Successfully commited Iceberg transaction for ranges ${batch.recordRanges.mkString(",")}")
  }

  override def committedPositions(topicPartitions: Set[TopicPartition]): Map[TopicPartition, Option[StreamPosition]] = {
    topicPartitions
      .map(tp => {
        tp -> Option(table.properties().get(offsetKey(tp.topic(), tp.partition()))).map(offsetWatermark => {
          val Array(o, w) = offsetWatermark.split(':')
          StreamPosition(o.toLong + 1, Timestamp(w.toLong))
        })
      })
      .toMap
  }
}

object IcebergRecordBatchStorage {

  case class Builder(private val _table: Table, private val _commitLock: Option[Lock]) {

    /**
      * Sets the Iceberg table to sync to.
      */
    def table(table: Table): Builder = copy(_table = table)

    /**
      * Sets a lock to use when commiting to Iceberg.
      */
    def commitLock(lock: Lock): Builder = copy(_commitLock = Some(lock))

    def build(): IcebergRecordBatchStorage = {
      if (_table == null) throw new IllegalArgumentException("Must provide a Table")

      new IcebergRecordBatchStorage(_table, _commitLock)
    }
  }

  def builder(): Builder = Builder(null, None)
}
