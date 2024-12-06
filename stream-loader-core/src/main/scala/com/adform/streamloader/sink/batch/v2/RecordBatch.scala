/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.sink.batch.v2

import com.adform.streamloader.model.{StreamPosition, StreamRange, StreamRangeBuilder, StreamRecord}
import com.adform.streamloader.sink.batch.RecordFormatter
import com.adform.streamloader.util.TimeProvider
import org.apache.kafka.common.TopicPartition

import java.io.Closeable
import java.time.Instant
import scala.collection.mutable

/**
  * A base trait representing a batch of records.
  */
trait RecordBatch extends Closeable {

  /**
    * Gets the ranges of records in each topic partition contained in the batch.
    */
  def recordRanges: Seq[StreamRange]

  def recordCount: Long

  override def close(): Unit = {}
}

trait InProgressPartitionedRecordBatch[P] extends InProgressBatch {
  def partitionBatches: Map[P, InProgressBatch]

  override def startTime: Instant = partitionBatches.values.map(_.startTime).min

  override def recordCount: Long = partitionBatches.values.map(_.recordCount).sum

  override def estimateSizeBytes(): Long = partitionBatches.values.map(_.estimateSizeBytes()).sum
}

trait RecordBatchBuilder[+B <: RecordBatch] extends BatchBuilder[StreamRecord, B]

abstract class BaseRecordBatchBuilder[+B <: RecordBatch](implicit timeProvider: TimeProvider = TimeProvider.system)
    extends BaseBatchBuilder[StreamRecord, B]()(timeProvider)
    with RecordBatchBuilder[B] {

  private val recordRangeBuilders: mutable.HashMap[TopicPartition, StreamRangeBuilder] = mutable.HashMap.empty
  protected def recordRanges: Seq[StreamRange] = recordRangeBuilders.values.map(_.build()).toSeq

  override def add(record: StreamRecord): Int = {
    val tp = new TopicPartition(record.consumerRecord.topic(), record.consumerRecord.partition())
    val position = StreamPosition(record.consumerRecord.offset(), record.watermark)
    val recordRangeBuilder =
      recordRangeBuilders.getOrElseUpdate(tp, new StreamRangeBuilder(tp.topic(), tp.partition(), position))
    recordRangeBuilder.extend(position)

    super.add(record)
  }
}
