/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.storage

import com.adform.streamloader.fixtures._
import com.adform.streamloader.model.{ExampleMessage, StreamPosition}
import com.adform.streamloader.util.UuidExtensions
import com.adform.streamloader.{BuildInfo, Loader}
import com.google.cloud.bigquery._
import org.apache.kafka.common.TopicPartition
import org.mandas.docker.client.DockerClient
import org.mandas.docker.client.messages.{ContainerConfig, HostConfig}
import org.scalacheck.Arbitrary

import java.time.{LocalDateTime, ZoneId}
import java.util.UUID
import scala.jdk.CollectionConverters._

case class BigQueryLoaderConfig(project: String, dataset: String, table: String) {
  def tableId: TableId = TableId.of(project, dataset, table)
  def tableSqlName: String = s"`$project`.`$dataset`.`$table`"
}

case class BigQueryStorageBackend(
    docker: DockerClient,
    dockerNetwork: DockerNetwork,
    kafkaContainer: ContainerWithEndpoint,
    bigQuery: BigQuery,
    loader: Loader,
    loaderConfig: BigQueryLoaderConfig
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
      Field
        .newBuilder("moneySpent", StandardSQLTypeName.NUMERIC)
        .setMode(Field.Mode.REQUIRED)
        .setScale(ExampleMessage.SCALE_PRECISION.scale)
        .setPrecision(ExampleMessage.SCALE_PRECISION.precision)
        .build()
    )

    val tableInfo = TableInfo.newBuilder(loaderConfig.tableId, StandardTableDefinition.of(schema)).build()
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
          .binds(
            HostConfig.Bind.builder()
              .from("/home/saulius/.config/gcloud/application_default_credentials.json")
              .to("/root/.config/gcloud/application_default_credentials.json")
              .build()
          )
          .build()
      )
      .env(
        s"APP_MAIN_CLASS=${loader.getClass.getName.replace("$", "")}",
        "APP_OPTS=-Dconfig.resource=application-bigquery.conf",
        s"KAFKA_BROKERS=${kafkaContainer.endpoint}",
        s"KAFKA_TOPIC=$topic",
        s"KAFKA_CONSUMER_GROUP=$consumerGroup",
        s"BIGQUERY_PROJECT=${loaderConfig.project}",
        s"BIGQUERY_DATASET=${loaderConfig.dataset}",
        s"BIGQUERY_TABLE=${loaderConfig.table}",
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
    partitions.map(tp => (tp, None)).toMap
  }

  override def getContent: StorageContent[ExampleMessage] = {
    val queryConfig = QueryJobConfiguration.newBuilder(s"select * from ${loaderConfig.tableSqlName}").build
    val result = bigQuery.query(queryConfig)

    val rows = result
      .iterateAll()
      .asScala
      .map(r => {
        ExampleMessage(
          r.get("id").getLongValue.toInt,
          r.get("name").getStringValue,
          LocalDateTime.ofInstant(r.get("timestamp").getTimestampInstant, ZoneId.of("UTC")),
          r.get("height").getDoubleValue,
          r.get("width").getDoubleValue.toFloat,
          r.get("isEnabled").getBooleanValue,
          r.get("childIds").getRepeatedValue.asScala.map(_.getLongValue.toInt).toArray,
          Option(r.get("parentId").getLongValue),
          UuidExtensions.fromBytes(r.get("transactionId").getBytesValue),
          r.get("moneySpent").getNumericValue
        )
      })
    StorageContent(rows.toSeq, Map.empty)
  }
}
