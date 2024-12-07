package com.adform.streamloader.sink.batch

import com.adform.streamloader.sink.batch.commit.BatchCommitStrategy
import com.adform.streamloader.sink.batch.storage.RecordBatchStorage
import com.adform.streamloader.sink.{PartitionGroupSinker, PartitionGroupingSink}
import com.adform.streamloader.source.KafkaContext
import com.adform.streamloader.util.Retry
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import scala.concurrent.duration.DurationInt
import scala.jdk.DurationConverters.JavaDurationOps

class RecordBatchingSink[+B <: RecordBatch](
    recordBatcher: RecordBatcher[B],
    batchStorage: RecordBatchStorage[B],
    batchCommitStrategy: BatchCommitStrategy,
    batchCommitQueueSize: Int,
    partitionGrouping: TopicPartition => String,
    retryPolicy: Retry.Policy
) extends PartitionGroupingSink {

  override def initialize(context: KafkaContext): Unit = {
    super.initialize(context)
    batchStorage.initialize(context)
  }

  final override def groupForPartition(topicPartition: TopicPartition): String =
    partitionGrouping(topicPartition)

  final override def sinkerForPartitionGroup(
      groupName: String,
      groupPartitions: Set[TopicPartition]
  ): PartitionGroupSinker =
    new RecordBatchingSinker[B](
      groupName,
      groupPartitions,
      recordBatcher,
      batchStorage,
      batchCommitStrategy,
      batchCommitQueueSize,
      retryPolicy
    )
}

object RecordBatchingSink {

  case class Builder[B <: RecordBatch](
      private val _recordBatcher: RecordBatcher[B],
      private val _batchStorage: RecordBatchStorage[B],
      private val _batchCommitStrategy: BatchCommitStrategy,
      private val _batchCommitQueueSize: Int,
      private val _partitionGrouping: TopicPartition => String,
      private val _retryPolicy: Retry.Policy
  ) {

    /**
      * Sets the record batcher to use.
      */
    def recordBatcher(batcher: RecordBatcher[B]): Builder[B] = copy(_recordBatcher = batcher)

    /**
      * Sets the storage, e.g. HDFS.
      */
    def batchStorage(storage: RecordBatchStorage[B]): Builder[B] = copy(_batchStorage = storage)

    def batchCommitStrategy(strategy: BatchCommitStrategy): Builder[B] = copy(_batchCommitStrategy = strategy)

    /**
      * Sets the max number of pending batches queued to be committed to storage.
      * Consumption stops when the queue fills up.
      */
    def batchCommitQueueSize(size: Int): Builder[B] = copy(_batchCommitQueueSize = size)

    /**
      * Sets the retry policy for all retriable operations, i.e. recovery, batch commit and new batch creation.
      */
    def retryPolicy(retries: Int, initialDelay: Duration, backoffFactor: Int): Builder[B] =
      copy(_retryPolicy = Retry.Policy(retries, initialDelay.toScala, backoffFactor))

    /**
      * Sets the partition grouping, can be used to route records to different batches.
      */
    def partitionGrouping(grouping: TopicPartition => String): Builder[B] = copy(_partitionGrouping = grouping)

    def build(): RecordBatchingSink[B] = {
      if (_recordBatcher == null) throw new IllegalStateException("Must specify a RecordBatcher")
      if (_batchStorage == null) throw new IllegalStateException("Must specify a RecordBatchStorage")

      new RecordBatchingSink[B](
        _recordBatcher,
        _batchStorage,
        _batchCommitStrategy,
        _batchCommitQueueSize,
        _partitionGrouping,
        _retryPolicy
      )
    }
  }

  def builder[B <: RecordBatch](): Builder[B] = Builder[B](
    _recordBatcher = null,
    _batchStorage = null,
    _batchCommitStrategy = null,
    _batchCommitQueueSize = 1,
    _partitionGrouping = _ => "root",
    _retryPolicy = Retry.Policy(retriesLeft = 5, initialDelay = 1.seconds, backoffFactor = 3)
  )
}
