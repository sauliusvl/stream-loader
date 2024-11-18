package com.adform.streamloader.sink.batch.v2

import com.adform.streamloader.model._
import com.adform.streamloader.sink.PartitionGroupSinker
import com.adform.streamloader.sink.batch.v2.storage.RecordBatchStorage
import com.adform.streamloader.source.KafkaContext
import com.adform.streamloader.util.Retry._
import com.adform.streamloader.util._
import io.micrometer.core.instrument.{Counter, Gauge, Meter, Timer}
import org.apache.kafka.common.TopicPartition

import java.time.Duration
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

class RecordBatchingSinker[B <: RecordBatch](
    override val groupName: String,
    override val groupPartitions: Set[TopicPartition],
    recordBatcher: RecordBatcher[B],
    batchStorage: RecordBatchStorage[B],
    batchCommitStrategy: BatchCommitStrategy,
    batchCommitQueueSize: Int,
    retryPolicy: Retry.Policy
) extends PartitionGroupSinker
    with Logging
    with Metrics {
  self =>

  override val metricsRoot = "stream_loader.batch"

  protected var kafkaContext: KafkaContext = _

  private var isInitialized = false
  private val isRunning = new AtomicBoolean(false)

  private var builder: RecordBatchBuilder[B] = _

  private val commitQueue = new ArrayBlockingQueue[B](batchCommitQueueSize)
  private val commitThread = new Thread(
    () => {
      while (isRunning.get()) try {

        def batchCommittedAfterFailure(batch: B): Boolean = retryOnFailure(retryPolicy) {
          batchStorage.recover(groupPartitions)
          batchStorage.isBatchCommitted(batch)
        }

        val batch = commitQueue.take()

        log.info(s"Committing batch $batch to storage")
        Metrics.commitDuration.recordCallable(() =>
          retryOnFailureIf(retryPolicy)(!batchCommittedAfterFailure(batch)) {
            batchStorage.commitBatch(batch)
          }
        )

        batch.recordRanges.foreach(range => {
          Metrics.committedWatermarks(range.topicPartition).set(range.end.watermark.millis)
        })

        batch.close()
      } catch {
        case e if isInterruptionException(e) =>
          log.debug("Batch commit thread interrupted")
      }
    },
    s"${Thread.currentThread().getName}-$groupName-batch-commit-thread" // e.g. loader-1-root-batch-commit-thread
  )

  def commitQueueSize: Int = commitQueue.size()

  override def initialize(context: KafkaContext): Map[TopicPartition, Option[StreamPosition]] = {
    if (isInitialized)
      throw new IllegalStateException(s"Loader for '$groupName' already initialized")

    kafkaContext = context

    log.info(s"Recovering storage for partitions ${groupPartitions.mkString(", ")}")

    retryOnFailure(retryPolicy) {
      batchStorage.recover(groupPartitions)
    }

    log.info(s"Looking up offsets for partitions ${groupPartitions.mkString(", ")}")
    val positions = batchStorage.committedPositions(groupPartitions)

    startNewBatch()

    isRunning.set(true)
    commitThread.start()

    isInitialized = true

    positions
  }

  override def write(record: StreamRecord): Unit = if (isRunning.get()) {
    if (!isInitialized)
      throw new IllegalStateException("Loader has to be initialized before starting writes")

    val batched = builder.add(record)

    Metrics.recordsWritten(record.topicPartition).increment()
    Metrics.recordsBatched(record.topicPartition).increment(batched)

    checkAndCommitBatchIfNeeded()
  }

  override def heartbeat(): Unit = checkAndCommitBatchIfNeeded()

  private def checkAndCommitBatchIfNeeded(): Unit = {
    if (batchCommitStrategy.shouldCommit(builder)) {
      batchCommitStrategy.preCommitBatch(builder)

      log.info(s"Forming batch for '$groupName' and putting it to the commit queue")
      builder.build().foreach { batch =>
        try {
          log.debug(s"Batch $batch formed, queuing it for commit to storage")
          commitQueue.put(batch)
        } catch {
          case e if isInterruptionException(e) =>
            log.debug("Loader interrupted while putting batch to commit queue")
            return
        }
      }

      startNewBatch()
    }
  }

  private def startNewBatch(): Unit = retryOnFailure(retryPolicy) {
    builder = recordBatcher.newBatchBuilder()
  }

  override def close(): Unit = if (isRunning.compareAndSet(true, false)) {
    log.info(s"Closing partition loader for '$groupName', discarding the current batch")
    builder.close()
    recordBatcher.close()

    log.debug("Interrupting batch commit thread and waiting for it to stop")
    commitThread.interrupt()
    commitThread.join()

    log.debug("Closing and removing meters")
    Metrics.allMeters.foreach(meter => {
      meter.close()
      removeMeters(meter)
    })
  }

  private object Metrics {

    private val commonTags = Seq(
      MetricTag("partition-group", groupName),
      MetricTag("loader-thread", Thread.currentThread().getName)
    )

    private def partitionTags(tp: TopicPartition) =
      Seq(MetricTag("topic", tp.topic()), MetricTag("partition", tp.partition().toString))

    val recordsWritten: Map[TopicPartition, Counter] =
      groupPartitions.map(tp => tp -> createCounter("records.written", commonTags ++ partitionTags(tp))).toMap

    val committedWatermarks: Map[TopicPartition, AssignableGauge[java.lang.Long]] =
      groupPartitions
        .map(tp =>
          tp -> createAssignableGauge(
            "committed.watermark.delay.ms",
            (latestWatermark: java.lang.Long) => (System.currentTimeMillis() - latestWatermark).toDouble,
            commonTags ++ partitionTags(tp)
          )
        )
        .toMap

    val recordsBatched: Map[TopicPartition, Counter] =
      groupPartitions.map(tp => tp -> createCounter("records.batched", commonTags ++ partitionTags(tp))).toMap

    val commitDuration: Timer = createTimer("commit.duration", commonTags, maxDuration = Duration.ofMinutes(5))
    val commitQueueSize: Gauge =
      createGauge("commit.queue.size", self, (_: RecordBatchingSinker[B]) => self.commitQueue.size(), commonTags)

    val allMeters: Seq[Meter] = Seq(commitDuration, commitQueueSize) ++
      recordsWritten.values ++ committedWatermarks.values.map(_.underlying) ++ recordsBatched.values
  }
}
