package com.adform.streamloader.sink.batch.v2

import java.time.Duration

trait BatchCommitStrategy {
  def shouldCommit(batch: InProgressBatch): Boolean
}

trait BatchSizeApproximator {
  def approximateSizeBytes(estimatedSizeBytes: Long): Long
}

object BatchSizeApproximator {
  object Identity extends BatchSizeApproximator {
    override def approximateSizeBytes(estimatedSizeBytes: Long): Long = estimatedSizeBytes
  }
}

object BatchCommitStrategy {
  case class ReachedAnyOf(
      openDuration: Option[Duration] = None,
      estimatedSizeBytes: Option[Long] = None,
      recordsWritten: Option[Long] = None
  ) extends BatchCommitStrategy {

    require(
      openDuration.isDefined || estimatedSizeBytes.isDefined || recordsWritten.isDefined,
      "At least one upper limit for the batch commit strategy has to be defined"
    )

    override def shouldCommit(batch: InProgressBatch): Boolean = {
      openDuration.exists(d => System.currentTimeMillis() - batch.startTime.toEpochMilli >= d.toMillis) ||
      estimatedSizeBytes.exists(b => batch.estimateSizeBytes() >= b) ||
      recordsWritten.exists(r => batch.recordCount >= r)
    }

    def withSizeApproximator(approx: BatchSizeApproximator): BatchCommitStrategy = {
      this
    }
  }
}
