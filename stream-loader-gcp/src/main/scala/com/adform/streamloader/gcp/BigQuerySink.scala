/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.gcp

import com.adform.streamloader.sink.batch.RecordFormatter
import com.adform.streamloader.sink.{PartitionGroupSinker, PartitionGroupingSink}
import com.google.cloud.bigquery.storage.v1.{BigQueryWriteClient, ProtoSchema, TableName}
import com.google.protobuf.Message
import org.apache.kafka.common.TopicPartition

class BigQuerySink(
    writeClient: BigQueryWriteClient,
    tableName: TableName,
    tableSchema: ProtoSchema,
    recordFormatter: RecordFormatter[Message]
) extends PartitionGroupingSink {

  override def groupForPartition(topicPartition: TopicPartition): String =
    s"${topicPartition.topic()}-${topicPartition.partition()}"

  override def sinkerForPartitionGroup(groupName: String, partitions: Set[TopicPartition]): PartitionGroupSinker =
    new BigQueryPartitionGroupSinker(groupName, partitions, writeClient, tableName, tableSchema, recordFormatter)
}
