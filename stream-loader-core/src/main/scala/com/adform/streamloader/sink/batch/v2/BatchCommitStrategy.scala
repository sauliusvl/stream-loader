package com.adform.streamloader.sink.batch.v2

import java.time.{Duration, Instant}

trait BatchCommitStrategy {
  def shouldCommit(batch: InProgressBatch): Boolean
  def preCommitBatch(batch: InProgressBatch): Unit = {}
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
  }

  private class WrappedInProgressBatch(batch: InProgressBatch) extends InProgressBatch {
    override def startTime: Instant = batch.startTime
    override def recordCount: Long = batch.recordCount
    override def estimateSizeBytes(): Long = batch.estimateSizeBytes()
  }

  private class WithSizeSampling(strategy: BatchCommitStrategy, sampleSize: Long) extends BatchCommitStrategy {
    private var numObserved: Long = 0L
    private var lastObserved: Long = 0L

    override def shouldCommit(batch: InProgressBatch): Boolean = {
      val wrapped = new WrappedInProgressBatch(batch) {
        override def estimateSizeBytes(): Long = {
          if (numObserved + 1 == sampleSize) {
            lastObserved = batch.estimateSizeBytes()
          }
          numObserved += 1
          lastObserved
        }
      }
      strategy.shouldCommit(wrapped)
    }
  }

  implicit class RichBatchCommitStrategy(strategy: BatchCommitStrategy) {
    def withSizeSampling(sampleSize: Long): BatchCommitStrategy = new WithSizeSampling(strategy, sampleSize)
    def withSizeApproximation(samples: Int): BatchCommitStrategy = strategy
    def fuzzy(): BatchCommitStrategy = strategy
  }
}
