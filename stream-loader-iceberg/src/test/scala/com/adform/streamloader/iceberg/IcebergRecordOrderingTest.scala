package com.adform.streamloader.iceberg

import com.adform.streamloader.iceberg.v2.IcebergRecordOrdering
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.types.Types
import org.apache.iceberg.types.Types.StructType
import org.apache.iceberg.{NullOrder, Schema, SortDirection, SortOrder}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, LocalDate}
import scala.jdk.CollectionConverters._

class IcebergRecordOrderingTest extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  val schema = new Schema(
    Types.NestedField.optional(100, "id", Types.IntegerType.get()),
    Types.NestedField.optional(1, "int", Types.IntegerType.get()),
    Types.NestedField.optional(2, "boolean", Types.BooleanType.get()),
    Types.NestedField.optional(3, "long", Types.LongType.get()),
    Types.NestedField.optional(4, "float", Types.FloatType.get()),
    Types.NestedField.optional(5, "double", Types.DoubleType.get()),
    Types.NestedField.optional(6, "date", Types.DateType.get()),
    Types.NestedField.optional(7, "time", Types.TimeType.get()),
    Types.NestedField.optional(8, "timestamp", Types.TimestampType.withZone()),
    Types.NestedField.optional(9, "timestamp_nano", Types.TimestampNanoType.withoutZone()),
    Types.NestedField.optional(10, "string", Types.StringType.get()),
    Types.NestedField.optional(11, "uuid", Types.UUIDType.get()),
    Types.NestedField.optional(12, "fixed", Types.FixedType.ofLength(10)),
    Types.NestedField.optional(13, "binary", Types.BinaryType.get()),
    Types.NestedField.optional(14, "decimal", Types.DecimalType.of(18, 10)),
    Types.NestedField.optional(20, "nested", StructType.of(
      Types.NestedField.optional(21, "inner", Types.StringType.get())
    ))
  )

  case class Records(values: Seq[Map[String, AnyRef]]) {
    def idsSortedBy(sortOrder: SortOrder): Seq[Int] = {
      val records = values.zipWithIndex.map { case (fields, idx) =>
        val fieldsWithId =  fields ++ Map("id" -> Integer.valueOf(idx + 1))
        GenericRecord.create(schema).copy(fieldsWithId.asJava)
      }
      records.sorted(new IcebergRecordOrdering(sortOrder, schema)).map(_.getField("id").asInstanceOf[Int])
    }
  }

  it("should respect sort direction") {
    val sortOrder = SortOrder
      .builderFor(schema)
      .sortBy(Expressions.ref("int"), SortDirection.DESC, NullOrder.NULLS_LAST)
      .build()

    val records = Records(Seq(
      Map("int" -> Integer.valueOf(1)),
      Map("int" -> Integer.valueOf(2)),
      Map("int" -> Integer.valueOf(3))
    ))

    records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(3, 2, 1)
  }

  it("should respect null order") {
    val sortOrder = SortOrder
      .builderFor(schema)
      .sortBy(Expressions.ref("int"), SortDirection.ASC, NullOrder.NULLS_LAST)
      .build()

    val records = Records(Seq(
      Map("int" -> Integer.valueOf(5)),
      Map("int" -> null),
      Map("int" -> Integer.valueOf(1))
    ))

    records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(3, 1, 2)
  }

  describe("primitive type ordering") {
    it("should work with int fields correctly") {
      val sortOrder = SortOrder
        .builderFor(schema)
        .sortBy(Expressions.ref("int"), SortDirection.ASC, NullOrder.NULLS_LAST)
        .build()

      val records = Records(Seq(
        Map("int" -> Integer.valueOf(4)),
        Map("int" -> Integer.valueOf(1)),
        Map("int" -> Integer.valueOf(5))
      ))

      records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(2, 1, 3)
    }

    it("should work with boolean fields correctly") {
      val sortOrder = SortOrder
        .builderFor(schema)
        .sortBy(Expressions.ref("boolean"), SortDirection.ASC, NullOrder.NULLS_FIRST)
        .build()

      val records = Records(Seq(
        Map("boolean" -> Boolean.box(true)),
        Map("boolean" -> Boolean.box(false)),
      ))

      records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(2, 1)
    }
  }

  describe("transformed field ordering") {
    it("should work with year(timestamp) correctly") {
      val sortOrder = SortOrder
        .builderFor(schema)
        .sortBy(Expressions.year("timestamp"), SortDirection.ASC, NullOrder.NULLS_FIRST)
        .build()

      val records = Records(Seq(
        Map("timestamp" -> java.lang.Long.valueOf(Instant.parse("2024-04-24T00:53:33.000Z").toEpochMilli * 1000)),
        Map("timestamp" -> java.lang.Long.valueOf(Instant.parse("2023-02-12T23:23:53.000Z").toEpochMilli * 1000)),
        Map("timestamp" -> java.lang.Long.valueOf(Instant.parse("2023-01-19T12:43:52.000Z").toEpochMilli * 1000)),
        Map("timestamp" -> java.lang.Long.valueOf(Instant.parse("2021-11-29T22:45:11.000Z").toEpochMilli * 1000)),
      ))

      records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(4, 2, 3, 1)
    }

    it("should work with year(date) correctly") {
      val sortOrder = SortOrder
        .builderFor(schema)
        .sortBy(Expressions.year("date"), SortDirection.ASC, NullOrder.NULLS_FIRST)
        .build()

      val records = Records(Seq(
        Map("date" -> Integer.valueOf(LocalDate.of(2024, 1, 20).toEpochDay.toInt)),
        Map("date" -> Integer.valueOf(LocalDate.of(2022, 2, 19).toEpochDay.toInt)),
        Map("date" -> Integer.valueOf(LocalDate.of(2023, 4, 10).toEpochDay.toInt)),
      ))

      records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(2, 3, 1)
    }
  }
}
