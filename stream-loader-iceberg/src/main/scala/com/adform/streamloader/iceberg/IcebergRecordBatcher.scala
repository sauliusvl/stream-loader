/*
 * Copyright (c) 2020 Adform
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package com.adform.streamloader.iceberg

import com.adform.streamloader.model.StreamRecord
import com.adform.streamloader.sink.batch.format.{FormattedRecordBatch, FormattedRecordBatchBuilder, RecordFormatter, RecordPartitioner}
import com.adform.streamloader.sink.batch.{BaseBatchBuilder, RecordBatcher}
import com.adform.streamloader.util.UuidExtensions.randomUUIDv7
import org.apache.iceberg._
import org.apache.iceberg.data.{GenericAppenderFactory, Record => IcebergRecord}
import org.apache.iceberg.io.DataWriteResult
import org.apache.iceberg.transforms.{SortOrderVisitor, Transform, Transforms}
import org.apache.iceberg.types.Type.TypeID

import java.nio.ByteBuffer
import java.util.Comparator
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

class IcebergRecordPartitioner(spec: PartitionSpec, schema: Schema)
    extends RecordPartitioner[IcebergRecord, PartitionKey] {
  def this(table: Table) = this(table.spec(), table.schema())

  override def partition(raw: StreamRecord, formatted: IcebergRecord): PartitionKey = {
    val pk = new PartitionKey(spec, schema)
    pk.partition(formatted)
    pk
  }
}

object IcebergRecordBatcher {

  case class Builder(
      private val _table: Table,
      private val _recordFormatter: RecordFormatter[IcebergRecord],
      private val _fileFormat: FileFormat,
      private val _writeProperties: Map[String, String]
  ) {

    /**
      * Sets the Iceberg table to build batches for.
      */
    def table(table: Table): Builder = copy(_table = table)

    /**
      * Sets the record formatter that converts from consumer records to Iceberg records.
      */
    def recordFormatter(formatter: RecordFormatter[IcebergRecord]): Builder = copy(_recordFormatter = formatter)

    /**
      * Sets the file format to use.
      */
    def fileFormat(format: FileFormat): Builder = copy(_fileFormat = format)

    /**
      * Sets any additional properties for the underlying data file builder.
      */
    def writeProperties(properties: Map[String, String]): Builder = copy(_writeProperties = properties)

    def build(): RecordBatcher[FormattedRecordBatch[PartitionKey, IcebergBatch]] = {
      if (_table == null) throw new IllegalStateException("Must specify a destination table")
      if (_recordFormatter == null) throw new IllegalStateException("Must specify a RecordFormatter")

      () =>
        new FormattedRecordBatchBuilder[IcebergRecord, PartitionKey, IcebergBatch](
          _recordFormatter,
          new IcebergRecordPartitioner(_table),
          pk => new IcebergBatchBuilder(_table, pk, _fileFormat, _writeProperties)
        )
    }
  }

  def builder(): Builder = Builder(
    _table = null,
    _recordFormatter = null,
    _fileFormat = FileFormat.PARQUET,
    _writeProperties = Map.empty
  )
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

      val primitiveComparator =
        if (direction == SortDirection.ASC) Comparator.naturalOrder[T]() else Comparator.naturalOrder[T].reversed()

      val primitiveNullableComparator =
        if (nullOrder == NullOrder.NULLS_FIRST) Comparator.nullsFirst(primitiveComparator)
        else Comparator.nullsLast(primitiveComparator)

      Comparator.comparing(
        (r: IcebergRecord) => fn.apply(accessor.get(r).asInstanceOf[S]),
        primitiveNullableComparator
      )
    }

    override def field(n: String, id: Int, dir: SortDirection, no: NullOrder): Comparator[IcebergRecord] = {
      transformed(Transforms.identity(), id, dir, no)
    }

    override def bucket(n: String, id: Int, b: Int, dir: SortDirection, no: NullOrder): Comparator[IcebergRecord] = {
      transformed(Transforms.bucket[AnyRef](b), id, dir, no)
    }

    override def truncate(n: String, id: Int, w: Int, dir: SortDirection, no: NullOrder): Comparator[IcebergRecord] = {
      schema.findField(id).`type`().typeId() match {
        case TypeID.INTEGER => transformed(Transforms.truncate[Integer](w), id, dir, no)
        case TypeID.LONG => transformed(Transforms.truncate[java.lang.Long](w), id, dir, no)
        case TypeID.STRING => transformed(Transforms.truncate[String](w), id, dir, no)
        case TypeID.DECIMAL => transformed(Transforms.truncate[java.math.BigDecimal](w), id, dir, no)
        case TypeID.BINARY => transformed(Transforms.truncate[ByteBuffer](w), id, dir, no)
        case _ => throw new IllegalArgumentException()
      }
    }

    override def year(n: String, id: Int, dir: SortDirection, no: NullOrder): Comparator[IcebergRecord] = {
      transformed(Transforms.year[AnyRef](), id, dir, no)
    }

    override def month(n: String, id: Int, dir: SortDirection, no: NullOrder): Comparator[IcebergRecord] = {
      transformed(Transforms.month[AnyRef](), id, dir, no)
    }

    override def day(n: String, id: Int, dir: SortDirection, no: NullOrder): Comparator[IcebergRecord] = {
      transformed(Transforms.day[AnyRef](), id, dir, no)
    }

    override def hour(n: String, id: Int, dir: SortDirection, no: NullOrder): Comparator[IcebergRecord] = {
      transformed(Transforms.hour[AnyRef](), id, dir, no)
    }
  }

  private lazy val comparator = SortOrderVisitor
    .visit(sortOrder, new ComparatorSortOrderVisitor(schema))
    .asScala
    .reduce((c1, c2) => c1.thenComparing(c2))

  override def compare(x: IcebergRecord, y: IcebergRecord): Int = if (sortOrder.isSorted) {
    comparator.compare(x, y)
  } else {
    0
  }
}
