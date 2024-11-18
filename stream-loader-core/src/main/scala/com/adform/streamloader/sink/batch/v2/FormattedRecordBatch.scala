package com.adform.streamloader.sink.batch.v2

import com.adform.streamloader.model.{StreamRange, StreamRecord}
import com.adform.streamloader.sink.batch.{RecordFormatter, RecordPartitioner}
import com.adform.streamloader.util.TimeProvider

import java.io.Closeable
import java.time.Instant
import scala.collection.mutable

case class FormattedRecordBatch[P, +B](
    partitionBatches: Map[P, B],
    recordCount: Long,
    recordRanges: Seq[StreamRange]
) extends RecordBatch {
  override def close(): Unit = partitionBatches.foreach {
    case (_, b: Closeable) => b.close()
    case _ =>
  }
}

trait InProgressFormattedRecordBatch[P] extends InProgressBatch {
  def partitionBatches: Map[P, InProgressBatch]

  override def startTime: Instant = partitionBatches.values.map(_.startTime).min

  override def recordCount: Long = partitionBatches.values.map(_.recordCount).sum

  override def estimateSizeBytes(): Long = partitionBatches.values.map(_.estimateSizeBytes()).sum
}

class FormattedRecordBatchBuilder[R, P, B](
    recordFormatter: RecordFormatter[R],
    recordPartitioner: RecordPartitioner[R, P],
    partitionBatchBuilder: P => BatchBuilder[R, B]
)(implicit timeProvider: TimeProvider = TimeProvider.system)
    extends BaseRecordBatchBuilder[FormattedRecordBatch[P, B]]()(timeProvider)
    with InProgressFormattedRecordBatch[P] {

  private val partitionBuilders: mutable.HashMap[P, BatchBuilder[R, B]] = mutable.HashMap.empty
  private var recordsWritten: Long = 0L

  override def partitionBatches: Map[P, InProgressBatch] = partitionBuilders.toMap

  override def estimateSizeBytes(): Long = partitionBuilders.map(_._2.estimateSizeBytes()).sum
  override def recordCount: Long = recordsWritten

  override def add(record: StreamRecord): Int = {
    super.add(record)

    val formatted = recordFormatter.format(record)
    var added = 0
    formatted.foreach(f => {
      val partition = recordPartitioner.partition(record, f)
      val partitionBuilder = partitionBuilders.getOrElseUpdate(
        partition,
        partitionBatchBuilder(partition)
      )
      added += 1
      partitionBuilder.add(f)
    })

    recordsWritten += added
    added
  }

  override def build(): Option[FormattedRecordBatch[P, B]] = {
    val batches = for {
      (partition, builder) <- partitionBuilders
      batch <- builder.build()
    } yield (partition, batch)

    if (batches.nonEmpty)
      Some(FormattedRecordBatch(batches.toMap, recordCount, recordRanges))
    else
      None
  }

  override def close(): Unit = partitionBuilders.values.foreach(_.close())
}
