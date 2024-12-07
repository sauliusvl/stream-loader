package com.adform.streamloader.sink.batch

import java.io.Closeable

trait RecordBatcher[+B <: RecordBatch] extends Closeable {

  /**
    * Gets a new record batch builder.
    */
  def newBatchBuilder(): RecordBatchBuilder[B]

  override def close(): Unit = {}
}
