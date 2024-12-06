package com.adform.streamloader.sink.batch.v2

import com.adform.streamloader.model.StreamRecord
import com.adform.streamloader.sink.batch.v2.formatting.{FormattedRecordBatch, FormattedRecordBatchBuilder}
import com.adform.streamloader.sink.batch.{RecordFormatter, RecordPartitioner}

import java.io.Closeable

trait RecordBatcher[+B <: RecordBatch] extends Closeable {

  /**
    * Gets a new record batch builder.
    */
  def newBatchBuilder(): RecordBatchBuilder[B]

  override def close(): Unit = {}
}

