package com.adform.streamloader.sink.batch.v2

import com.adform.streamloader.sink.batch.{RecordFormatter, RecordPartitioner}

import java.io.Closeable

trait RecordBatcher[+B <: RecordBatch] extends Closeable {

  /**
    * Gets a new record batch builder.
    */
  def newBatchBuilder(): RecordBatchBuilder[B]

  override def close(): Unit = {}
}

object FormattingRecordBatcher {
  case class Builder[R, P, B](
      _formatter: RecordFormatter[R],
      _partitioner: RecordPartitioner[R, P],
      _builder: P => BatchBuilder[R, B]
  ) {

    def partitioner(pt: RecordPartitioner[R, P]): Builder[R, P, B] = copy(_partitioner = pt)

    def partitionBatchBuilder(builder: P => BatchBuilder[R, B]): Builder[R, P, B] = copy(_builder = builder)

    def batchBuilder(builder: () => BatchBuilder[R, B]): Builder[R, P, B] = copy(_builder = (_: P) => builder())

    def formatter(fmt: RecordFormatter[R]): Builder[R, P, B] = copy(_formatter = fmt)

    def build(): RecordBatcher[FormattedRecordBatch[P, B]] =
      () => new FormattedRecordBatchBuilder[R, P, B](_formatter, _partitioner, _builder)
  }

  def builder[R, P, B](): Builder[R, P, B] = Builder(null, null, null)
}
