/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.loaders

import com.adform.streamloader.gcp.BigQuerySink
import com.adform.streamloader.model.{ExampleMessage, StreamRecord, Timestamp}
import com.adform.streamloader.sink.batch.RecordFormatter
import com.adform.streamloader.source.KafkaSource
import com.adform.streamloader.util.ConfigExtensions._
import com.adform.streamloader.util.ProtoConverter
import com.adform.streamloader.{Loader, StreamLoader}
import com.google.cloud.bigquery.storage.v1.{BigQueryWriteClient, ProtoSchema, TableName}
import com.google.protobuf.Message
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.slf4j.bridge.SLF4JBridgeHandler

import java.util.Optional

object TestBigQueryLoader extends Loader {
  def main(args: Array[String]): Unit = {

    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()

//    val bigquery = BigQueryOptions.getDefaultInstance.getService
//
//    val tbl = bigquery.getTable(TableId.of("dv-grf-plyg-sb", "not_used", "example_test"))
//    tbl.getDefinition[StandardTableDefinition]


//
//    val schema = Schema.of(
//      Field.newBuilder("id", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
//      Field.newBuilder("name", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
//      Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REQUIRED).build(),
//      Field.newBuilder("height", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
//      Field.newBuilder("width", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
//      Field.newBuilder("isEnabled", StandardSQLTypeName.BOOL).setMode(Field.Mode.REQUIRED).build(),
//      Field.newBuilder("childIds", StandardSQLTypeName.INT64).setMode(Field.Mode.REPEATED).build(),
//      Field.newBuilder("parentId", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
//      Field.newBuilder("transactionId", StandardSQLTypeName.BYTES).setMode(Field.Mode.REQUIRED).build(),
//      Field
//        .newBuilder("moneySpent", StandardSQLTypeName.NUMERIC)
//        .setMode(Field.Mode.REQUIRED)
//        .setScale(ExampleMessage.SCALE_PRECISION.scale)
//        .setPrecision(ExampleMessage.SCALE_PRECISION.precision)
//        .build()
//    )
//
//    val tableIdObj = TableId.of("dv-grf-plyg-sb", "not_used", "example_test")
//    val tableInfo = TableInfo.newBuilder(tableIdObj, StandardTableDefinition.of(schema)).build()
//
////    bigquery.delete(tableIdObj)
////    bigquery.create(tableInfo)
//
//    val queryConfig = QueryJobConfiguration.newBuilder("select * from `dv-grf-plyg-sb`.not_used.example_test").build
//    val result = bigquery.query(queryConfig)
//
//    result
//      .iterateAll()
//      .forEach(r => {
//        println(r)
//      })
//
//    return

    val cfg = ConfigFactory.load().getConfig("stream-loader")
    val bqCfg = cfg.getConfig("bigquery")

    val source = KafkaSource
      .builder()
      .consumerProperties(cfg.getConfig("kafka.consumer").toProperties)
      .pollTimeout(cfg.getDuration("kafka.poll-timeout"))
      .topics(Seq(cfg.getString("kafka.topic")))
      .build()

    val bqWriteClient = BigQueryWriteClient.create()

    val protoConverter = new ProtoConverter[ExampleMessage]
    val protoSchema = ProtoSchema
      .newBuilder()
      .setProtoDescriptor(protoConverter.descriptorProto)
      .build()

    val recordFormatter: RecordFormatter[Message] = (r: StreamRecord) => {
      val msg = ExampleMessage.parseFrom(r.consumerRecord.value())
      Seq(protoConverter.toProto(msg))
    }

    val sink = new BigQuerySink(
      bqWriteClient,
      TableName.of(bqCfg.getString("project"), bqCfg.getString("dataset"), bqCfg.getString("table")),
      protoSchema,
      recordFormatter
    )
//
//    val ps = new BigQueryPartitionGroupSinker(
//      "root",
//      Set(new TopicPartition("tp", 0)),
//      bqWriteClient,
//      TableName.of(bqCfg.getString("project"), bqCfg.getString("dataset"), bqCfg.getString("table")),
//      protoSchema,
//      recordFormatter
//    )
//
//    ps.initialize(null)
//
//    val msg = ExampleMessage(
//      1,
//      "test",
//      LocalDateTime.now(),
//      1.1,
//      2.2f,
//      true,
//      Array(6, 7, 8),
//      Some(666),
//      UUID.randomUUID(),
//      BigDecimal(79)
//    )
//
//    ps.write(newStreamRecord("tp", 0, 0, Timestamp(1L), null, msg.getBytes))
//    ps.write(newStreamRecord("tp", 0, 0, Timestamp(1L), null, msg.getBytes))
//
//    ps.close()
//
    val loader = new StreamLoader(source, sink)

    sys.addShutdownHook {
      loader.stop()
      bqWriteClient.shutdown()
    }

    loader.start()
  }

  def newStreamRecord(
      topic: String,
      partition: Int,
      offset: Long,
      timestamp: Timestamp,
      key: Array[Byte],
      value: Array[Byte]
  ): StreamRecord = {
    val cr = new ConsumerRecord[Array[Byte], Array[Byte]](
      topic,
      partition,
      offset,
      timestamp.millis,
      TimestampType.CREATE_TIME,
      -1,
      -1,
      key,
      value,
      new RecordHeaders,
      Optional.empty[Integer]
    )
    StreamRecord(cr, timestamp)
  }
}
