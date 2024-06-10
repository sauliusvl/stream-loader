/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.storage

import com.adform.streamloader.fixtures._
import com.adform.streamloader.model.{ExampleMessage, StreamPosition, StringMessage, Timestamp}
import com.adform.streamloader.s3.S3FileStorage
import com.adform.streamloader.sink.file.{FilePathFormatter, TimePartitioningFilePathFormatter}
import com.adform.streamloader.{BuildInfo, Loader}
import com.google.cloud.bigquery.{
  BigQuery,
  Field,
  Schema,
  StandardSQLTypeName,
  StandardTableDefinition,
  TableId,
  TableInfo
}
import org.mandas.docker.client.DockerClient
import org.mandas.docker.client.messages.{ContainerConfig, HostConfig}
import org.apache.kafka.common.TopicPartition
import org.scalacheck.Arbitrary
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._
import software.amazon.awssdk.utils.IoUtils

import java.time.LocalDate
import java.util.UUID
import scala.jdk.CollectionConverters._

case class BigQueryStorageBackend(
    docker: DockerClient,
    dockerNetwork: DockerNetwork,
    kafkaContainer: ContainerWithEndpoint,
    loader: Loader,
    bigQuery: BigQuery,
    table: String
) extends StorageBackend[ExampleMessage] {

  override def arbMessage: Arbitrary[ExampleMessage] = ExampleMessage.arbMessage

  override def initialize(): Unit = {
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

    bigQuery.create(tableInfo)
  }

  def createLoaderContainer(loaderKafkaConfig: LoaderKafkaConfig, batchSize: Long): Container = {
    val consumerGroup = loaderKafkaConfig.consumerGroup
    val topic = loaderKafkaConfig.topic
    val loaderName = s"bigquery-loader-${UUID.randomUUID().toString.take(6)}"

    val config = ContainerConfig
      .builder()
      .image(BuildInfo.dockerImage)
      .hostConfig(
        HostConfig
          .builder()
          .networkMode(dockerNetwork.id)
          .build()
      )
      .env(
        s"APP_MAIN_CLASS=${loader.getClass.getName.replace("$", "")}",
        "APP_OPTS=-Dconfig.resource=application-bigquery.conf",
        s"KAFKA_BROKERS=${kafkaContainer.endpoint}",
        s"KAFKA_TOPIC=$topic",
        s"KAFKA_CONSUMER_GROUP=$consumerGroup",
//        s"S3_ENDPOINT=http://${s3Container.ip}:${s3Container.port}",
//        s"S3_ACCESS_KEY=${s3Config.accessKey}",
//        s"S3_SECRET_KEY=${s3Config.secretKey}",
//        s"S3_BUCKET=${s3Config.bucket}",
        s"BATCH_SIZE=$batchSize"
      )
      .build()

    val containerCreation = docker.createContainer(config, loaderName)
    SimpleContainer(containerCreation.id, loaderName)
  }

  def committedPositions(
      loaderKafkaConfig: LoaderKafkaConfig,
      partitions: Set[TopicPartition]
  ): Map[TopicPartition, Option[StreamPosition]] = {

    val kafkaContext = getKafkaContext(kafkaContainer, loaderKafkaConfig.consumerGroup)
//    val storage = GcsFileStorage
//      .builder()
//      .s3Client(s3Client)
//      .bucket(s3Config.bucket)
//      .filePathFormatter(pathFormatter)
//      .build()
//
//    storage.initialize(kafkaContext)
//    storage.committedPositions(partitions)
    Map.empty
  }

  override def getContent: StorageContent[ExampleMessage] = {
    StorageContent(Seq.empty, Map.empty)
  }
}
