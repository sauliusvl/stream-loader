package com.adform.streamloader.sink.batch.v2.formatting

import com.adform.streamloader.model.StreamRecord
import com.adform.streamloader.sink.batch.{RecordFormatter, RecordPartitioner}
import com.adform.streamloader.sink.batch.v2.{BatchBuilder, RecordBatcher}

object FormattingRecordBatcher {

  class RawBuilder {
    def formatter[R](fmt: RecordFormatter[R]): UnpartitionedBuilder[R] = new UnpartitionedBuilder(fmt)
  }

  class UnpartitionedBuilder[R](_formatter: RecordFormatter[R]) {
    def partitioner[P](pt: RecordPartitioner[R, P]): PartitionedBuilder[R, P] = new PartitionedBuilder(_formatter, pt)
    def batchBuilder[B](builder: () => BatchBuilder[R, B]): Builder[R, Unit, B] = new Builder(_formatter, (raw: StreamRecord, formatted: R) => (), (_: Unit) => builder())
  }

  class PartitionedBuilder[R, P](_formatter: RecordFormatter[R], _partitioner: RecordPartitioner[R, P]) {
    def batchBuilder[B](builder: P => BatchBuilder[R, B]): Builder[R, P, B] = new Builder(_formatter, _partitioner, builder)
  }

  class Builder[R, P, B](
                          _formatter: RecordFormatter[R],
                          _partitioner: RecordPartitioner[R, P],
                          _builder: P => BatchBuilder[R, B]
                        ) {
    def build(): RecordBatcher[FormattedRecordBatch[P, B]] =
      () => new FormattedRecordBatchBuilder[R, P, B](_formatter, _partitioner, _builder)
  }

  def builder(): RawBuilder = new RawBuilder()
}
