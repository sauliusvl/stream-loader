package com.adform.streamloader.sink.batch

import com.adform.streamloader.util.TimeProvider

import java.io.Closeable
import java.time.Instant
import scala.collection.mutable

trait InProgressBatch {
  def startTime: Instant

  def recordCount: Long

  def estimateSizeBytes(): Long
}

trait BatchBuilder[-R, +B] extends InProgressBatch with Closeable {

  def add(record: R): Int

  def build(): Option[B]

  override def close(): Unit = {}
}

abstract class BaseBatchBuilder[-R, +B](implicit timeProvider: TimeProvider = TimeProvider.system)
    extends BatchBuilder[R, B] {
  override def startTime: Instant = timeProvider.currentTime

  private var recordsWritten: Long = 0L

  override def add(record: R): Int = {
    recordsWritten += 1
    1
  }

  override def recordCount: Long = recordsWritten
}

class SortingBatchBuilder[R, +B](base: BatchBuilder[R, B], ordering: Ordering[R]) extends BaseBatchBuilder[R, B] {

  private val items = mutable.ListBuffer.empty[R]

  override def add(record: R): Int = {
    items.addOne(record)
    1
  }

  override def build(): Option[B] = {
    val sorted = items.sorted(ordering)
    sorted.foreach(base.add)
    base.build()
  }

  override def estimateSizeBytes(): Long = base.estimateSizeBytes()
}
