/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.loaders

import com.adform.streamloader.gcp.{BigQueryPartitionGroupSinker, BigQuerySink}
import com.adform.streamloader.model.{ExampleMessage, StreamRecord, Timestamp}
import com.adform.streamloader.sink.batch.RecordFormatter
import com.adform.streamloader.source.KafkaSource
import com.adform.streamloader.util.AvroProtoConverter
import com.adform.streamloader.util.ConfigExtensions._
import com.adform.streamloader.{Loader, StreamLoader}
import com.google.cloud.bigquery.{
  BigQueryOptions,
  Field,
  QueryJobConfiguration,
  Schema,
  StandardSQLTypeName,
  StandardTableDefinition,
  TableDefinition,
  TableId,
  TableInfo
}
import com.google.cloud.bigquery.storage.v1.{BigQueryWriteClient, BigQueryWriteSettings, ProtoSchema, TableName}
import com.google.protobuf.{DescriptorProtos, DynamicMessage, Message}
import com.google.protobuf.DescriptorProtos.{DescriptorProto, FieldDescriptorProto}
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.TimestampType
import org.slf4j.bridge.SLF4JBridgeHandler

import java.time.LocalDateTime
import java.util.{Optional, UUID}

object TestBigQueryLoader extends Loader {
  def main(args: Array[String]): Unit = {

    SLF4JBridgeHandler.removeHandlersForRootLogger()
    SLF4JBridgeHandler.install()

    val bigquery = BigQueryOptions.getDefaultInstance.getService

    val schema = Schema.of(
      Field.newBuilder("id", StandardSQLTypeName.INT64).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("name", StandardSQLTypeName.STRING).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("timestamp", StandardSQLTypeName.TIMESTAMP).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("height", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("width", StandardSQLTypeName.FLOAT64).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("isEnabled", StandardSQLTypeName.BOOL).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("childIds", StandardSQLTypeName.INT64).setMode(Field.Mode.REPEATED).build(),
      Field.newBuilder("parentId", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("transactionId", StandardSQLTypeName.BYTES).setMode(Field.Mode.REQUIRED).build(),
      Field.newBuilder("moneySpent", StandardSQLTypeName.BYTES).setMode(Field.Mode.REQUIRED).build()
    )

    val tableIdObj = TableId.of("dv-grf-plyg-sb", "not_used", "example_test")
    val tableInfo = TableInfo.newBuilder(tableIdObj, StandardTableDefinition.of(schema)).build()

    // bigquery.create(tableInfo)

    val queryConfig = QueryJobConfiguration.newBuilder("select * from `dv-grf-plyg-sb`.not_used.example_test").build
    val result = bigquery.query(queryConfig)

    result
      .iterateAll()
      .forEach(r => {
        println(r)
      })

    return
    // Access fields by name or index
//      val name: String = row.get("name").getStringValue
//      val age: Long = row.get("age").getLongValue
//      val city: String = row.get("city").getStringValue

    // println(s"Name: $name, Age: $age, City: $city")
    // }

    val cfg = ConfigFactory.load().getConfig("stream-loader")

//    val source = KafkaSource
//      .builder()
//      .consumerProperties(cfg.getConfig("kafka.consumer").toProperties)
//      .pollTimeout(cfg.getDuration("kafka.poll-timeout"))
//      .topics(Seq(cfg.getString("kafka.topic")))
//      .build()

    val bqWriteClient = BigQueryWriteClient.create()

    val protoConverter = new AvroProtoConverter[ExampleMessage]
    val protoSchema = ProtoSchema
      .newBuilder()
      .setProtoDescriptor(protoConverter.descriptorProto)
      .build()

    val recordFormatter: RecordFormatter[Message] = (r: StreamRecord) => {
      val msg = ExampleMessage.parseFrom(r.consumerRecord.value())
      Seq(protoConverter.toProto(msg))
    }
//
//    val sink = new BigQuerySink(
//      bqWriteClient,
//      TableName.parse(cfg.getString("bigquery.table")),
//      protoSchema,
//      recordFormatter,
//      _ => "root"
//    )

    val ps = new BigQueryPartitionGroupSinker(
      "root",
      Set(new TopicPartition("tp", 0)),
      bqWriteClient,
      TableName.parse(cfg.getString("bigquery.table")),
      protoSchema,
      recordFormatter
    )

    ps.initialize(null)

    val msg = ExampleMessage(
      1,
      "test",
      LocalDateTime.now(),
      1.1,
      2.2f,
      true,
      Array(6, 7, 8),
      Some(666),
      UUID.randomUUID(),
      BigDecimal(79)
    )

    ps.write(newStreamRecord("tp", 0, 0, Timestamp(1L), null, msg.getBytes))

    ps.close()
//
//    val loader = new StreamLoader(source, sink)
//
//    sys.addShutdownHook {
//      loader.stop()
//      bqWriteClient.shutdown()
//    }
//
//    loader.start()
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
