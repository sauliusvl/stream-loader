/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader

import com.adform.streamloader.behaviors.{BasicLoaderBehaviors, KafkaRestartBehaviors, RebalanceBehaviors}
import com.adform.streamloader.fixtures._
import com.adform.streamloader.loaders.TestBigQueryLoader
import com.adform.streamloader.storage.{BigQueryLoaderConfig, BigQueryStorageBackend}
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, DatasetId, DatasetInfo}
import org.scalatest.concurrent.Eventually
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.tags.Slow
import org.scalatestplus.scalacheck.Checkers

import scala.concurrent.ExecutionContext

@Slow
class BigQueryIntegrationTests
    extends AnyFunSpec
    with Matchers
    with Eventually
    with Checkers
    with DockerTestFixture
    with KafkaTestFixture
    with Loaders
    with BasicLoaderBehaviors
    with RebalanceBehaviors
    with KafkaRestartBehaviors {

  implicit val context: ExecutionContext = ExecutionContext.global

  val kafkaConfig: KafkaConfig = KafkaConfig()

  var bigQuery: BigQuery = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    bigQuery = BigQueryOptions.getDefaultInstance.getService

    val dataset = DatasetId.of("dv-grf-plyg-sb", "loader_tests")
    if (!bigQuery.getDataset(dataset).exists()) {
      bigQuery.create(DatasetInfo.newBuilder(dataset).build())
    }
  }

  override def afterAll(): Unit = {
    // bigQuery.delete(DatasetId.of("dv-grf-plyg-sb", "loader_tests"))
    super.afterAll()
  }

  def bigQueryBackend(loader: Loader)(testId: String): BigQueryStorageBackend = {
    val backend = BigQueryStorageBackend(
      docker,
      dockerNetwork,
      kafkaContainer,
      bigQuery,
      loader,
      BigQueryLoaderConfig("dv-grf-plyg-sb", "loader_tests", testId)
    )
    backend.initialize()
    backend
  }

  it should behave like basicLoader("BigQuery loader", bigQueryBackend(TestBigQueryLoader))
}
