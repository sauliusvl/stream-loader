/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.iceberg.v2

import com.adform.streamloader.model.StreamRecord
import com.adform.streamloader.sink.batch.RecordPartitioner
import com.adform.streamloader.sink.batch.v2._
import com.adform.streamloader.util.UuidExtensions.randomUUIDv7
import org.apache.iceberg.data.{GenericAppenderFactory, Record => IcebergRecord}
import org.apache.iceberg.io.DataWriteResult
import org.apache.iceberg.{FileFormat, PartitionKey, SortOrder, Table}

import scala.jdk.CollectionConverters._

case class IcebergBatch(dataWriteResult: DataWriteResult)

class IcebergBatchBuilder(
    table: Table,
    pk: PartitionKey,
    fileFormat: FileFormat,
    writeProperties: Map[String, String]
) extends BaseBatchBuilder[IcebergRecord, IcebergBatch] {

  private val dataWriter = {
    val filename = fileFormat.addExtension(randomUUIDv7().toString)
    val path = table.locationProvider().newDataLocation(table.spec(), pk, filename)
    val output = table.io().newOutputFile(path)

    val factory = new GenericAppenderFactory(table.schema(), table.spec())
    factory.setAll(writeProperties.asJava)

    val encrypted = table.encryption().encrypt(output)
    factory.newDataWriter(encrypted, fileFormat, pk)
  }

  override def add(record: IcebergRecord): Int = {
    dataWriter.write(record)
    super.add(record)
  }

  override def build(): Option[IcebergBatch] = {
    dataWriter.close()
    if (recordCount > 0) Some(IcebergBatch(dataWriter.result())) else None
  }

  override def close(): Unit = {
    dataWriter.close()
  }

  override def estimateSizeBytes(): Long = dataWriter.length()
}

class IcebergRecordPartitioner(table: Table) extends RecordPartitioner[IcebergRecord, PartitionKey] {
  override def partition(raw: StreamRecord, formatted: IcebergRecord): PartitionKey = {
    val pk = new PartitionKey(table.spec(), table.schema())
    pk.partition(formatted)
    pk
  }
}

class IcebergRecordOrdering(orderSpec: SortOrder) extends Ordering[IcebergRecord] {
  def this(table: Table) = this(table.sortOrder())

  override def compare(x: IcebergRecord, y: IcebergRecord): Int = x.get(0).toString.toInt.compare(y.get(0).toString.toInt)
}
