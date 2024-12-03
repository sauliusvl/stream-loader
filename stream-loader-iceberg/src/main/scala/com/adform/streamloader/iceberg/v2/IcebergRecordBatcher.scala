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
import org.apache.iceberg.transforms.{Days, SortOrderVisitor, Transform, Transforms}
import org.apache.iceberg.types.Type.TypeID
import org.apache.iceberg.types.Types
import org.apache.iceberg.{FileFormat, NullOrder, PartitionKey, PartitionSpec, Schema, SortDirection, SortField, SortOrder, Table}

import java.util.Comparator
import java.util.function.ToIntFunction
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

class IcebergRecordPartitioner(spec: PartitionSpec, schema: Schema) extends RecordPartitioner[IcebergRecord, PartitionKey] {
  def this(table: Table) = this(table.spec(), table.schema())

  override def partition(raw: StreamRecord, formatted: IcebergRecord): PartitionKey = {
    val pk = new PartitionKey(spec, schema)
    pk.partition(formatted)
    pk
  }
}

class IcebergRecordOrdering(sortOrder: SortOrder, schema: Schema) extends Ordering[IcebergRecord] {
  def this(table: Table) = this(table.sortOrder(), table.schema())

  private class ComparatorSortOrderVisitor(schema: Schema) extends SortOrderVisitor[Comparator[IcebergRecord]] {

    private def transformed[S, T <: Comparable[T]](
      transform: Transform[S, T],
      sourceId: Int,
      direction: SortDirection,
      nullOrder: NullOrder
    ): Comparator[IcebergRecord] = {

      val field = schema.findField(sourceId)
      val accessor = schema.accessorForField(sourceId)
      val fn = transform.bind(field.`type`())

      val primitiveComparator = if (nullOrder == NullOrder.NULLS_FIRST)
        Comparator.nullsFirst(Comparator.naturalOrder[T]())
      else
        Comparator.nullsLast(Comparator.naturalOrder[T]())

      val cmp: Comparator[IcebergRecord] = Comparator.comparing(
        (r: IcebergRecord) => fn.apply(accessor.get(r).asInstanceOf[S]),
        primitiveComparator
      )

      if (direction == SortDirection.ASC) cmp else cmp.reversed()
    }

    override def field(sourceName: String, sourceId: Int, direction: SortDirection, nullOrder: NullOrder): Comparator[IcebergRecord] = {
      transformed(Transforms.identity(), sourceId, direction, nullOrder)
    }

    override def bucket(sourceName: String, sourceId: Int, width: Int, direction: SortDirection, nullOrder: NullOrder): Comparator[IcebergRecord] = {
      ???
    }

    override def truncate(sourceName: String, sourceId: Int, width: Int, direction: SortDirection, nullOrder: NullOrder): Comparator[IcebergRecord] = ???

    override def year(sourceName: String, sourceId: Int, direction: SortDirection, nullOrder: NullOrder): Comparator[IcebergRecord] = {
      transformed(Transforms.year[AnyRef](), sourceId, direction, nullOrder)
    }

    override def month(sourceName: String, sourceId: Int, direction: SortDirection, nullOrder: NullOrder): Comparator[IcebergRecord] = ???

    override def day(sourceName: String, sourceId: Int, direction: SortDirection, nullOrder: NullOrder): Comparator[IcebergRecord] = ???

    override def hour(sourceName: String, sourceId: Int, direction: SortDirection, nullOrder: NullOrder): Comparator[IcebergRecord] = ???
  }

  private val comparator = SortOrderVisitor
    .visit(sortOrder, new ComparatorSortOrderVisitor(schema))
    .asScala
    .reduce((c1, c2) => c1.thenComparing(c2))

  override def compare(x: IcebergRecord, y: IcebergRecord): Int = comparator.compare(x, y)
}
