/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.gcp

import com.adform.streamloader.model.{StreamPosition, StreamRecord}
import com.adform.streamloader.sink.PartitionGroupSinker
import com.adform.streamloader.sink.batch.RecordFormatter
import com.adform.streamloader.source.KafkaContext
import com.adform.streamloader.util.{Logging, Metrics}
import com.google.api.core.{ApiFutureCallback, ApiFutures}
import com.google.cloud.bigquery.storage.v1._
import com.google.common.util.concurrent.MoreExecutors
import com.google.protobuf.{Int64Value, Message}
import org.apache.kafka.common.TopicPartition

class BigQueryPartitionGroupSinker(
    val groupName: String,
    val groupPartitions: Set[TopicPartition],
    writeClient: BigQueryWriteClient,
    tableName: TableName,
    tableSchema: ProtoSchema,
    recordFormatter: RecordFormatter[Message]
) extends PartitionGroupSinker
    with Logging
    with Metrics {

  private var writeStream: WriteStream = _
  private var streamWriter: StreamWriter = _

  private var i = 0L

  override def initialize(kafkaContext: KafkaContext): Map[TopicPartition, Option[StreamPosition]] = {
    val stream = WriteStream.newBuilder
      .setType(WriteStream.Type.BUFFERED)
      .build()

    val createWriteStreamRequest = CreateWriteStreamRequest.newBuilder
      .setParent(tableName.toString)
      .setWriteStream(stream)
      .build()

    writeStream = writeClient.createWriteStream(createWriteStreamRequest)

    log.info(s"Created BigQuery stream ${writeStream.getName}")

    streamWriter = StreamWriter
      .newBuilder(writeStream.getName, writeClient)
      .setWriterSchema(tableSchema)
      .build()

    Map.empty
  }

  override def write(record: StreamRecord): Unit = {

    var rows = ProtoRows.newBuilder()

    recordFormatter
      .format(record)
      .foreach(message => {
        rows.addSerializedRows(message.toByteString)
      })

//    val batchSize = 6000
//
//    if ((i + 1) % batchSize == 0) {

    val future = streamWriter.append(rows.build(), 0)
    // val future = streamWriter.append(rows.build(), (i + 1) - batchSize)

    ApiFutures.addCallback(
      future,
      new ApiFutureCallback[AppendRowsResponse] {
        override def onFailure(t: Throwable): Unit = {
          println(s"Failed:" + t.getMessage)
        }

        override def onSuccess(result: AppendRowsResponse): Unit = {
          println(s"Success: ${result.getAppendResult.getOffset}")
//          lock.synchronized {
//            if (y > latest) latest = y
//            //println(s"Overriding latest to $latest")
//          }

        }
      },
      MoreExecutors.directExecutor()
    )

    val r = future.get()
    // println(s"Append succeeded at $y: ${r.getAppendResult.getOffset}")

    // rows = ProtoRows.newBuilder()
    // }

//    i += 1
//    val r = 4 * batchSize
//
//    if (i > 0 && i % r == 0) {
    // val now = System.currentTimeMillis()
    // println("Submit Rate " + r.toDouble / (now - start) * 1000 + " msgs/s")
    // start = now

    // if (latest > lastCommitted) {
//        val diff = latest - lastCommitted
//        val now = System.currentTimeMillis()
//        println(s"[$nr] Commit Rate " + (diff).toDouble / (now - start) * 1000 + " msgs/s")
//        start = now

    // println(s"Committing at ${latest}...")
//        try {
    val flushRowsRequest =
      FlushRowsRequest
        .newBuilder()
        .setWriteStream(writeStream.getName)
        .setOffset(Int64Value.of(0)) // i - 1))
        .build()

    writeClient.flushRows(flushRowsRequest)
//          lastCommitted = latest
//        } catch {
//          case e: Exception =>
//            println("Not yet: " + e.getMessage)
//          //Thread.sleep(1000)
//        }

//        totalCountLock.synchronized {
//          totalCount += diff
//        }

    // }
  }

  override def heartbeat(): Unit = {}

  override def close(): Unit = {
    streamWriter.close()
    writeClient.finalizeWriteStream(writeStream.getName)
  }

  override protected def metricsRoot: String = "stream_loader.bigquery"
}
