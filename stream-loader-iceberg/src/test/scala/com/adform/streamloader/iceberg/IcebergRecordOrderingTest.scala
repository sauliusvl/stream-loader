package com.adform.streamloader.iceberg

import com.adform.streamloader.iceberg.v2.IcebergRecordOrdering
import org.apache.iceberg.data.GenericRecord
import org.apache.iceberg.expressions.{Expressions, UnboundTerm}
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
    Types.NestedField.optional(
      20,
      "nested",
      StructType.of(
        Types.NestedField.optional(21, "inner", Types.StringType.get())
      )
    )
  )

  case class Records(values: Seq[Map[String, AnyRef]]) {
    def idsSortedBy(sortOrder: SortOrder): Seq[Int] = {
      val records = values.zipWithIndex.map { case (fields, idx) =>
        val fieldsWithId = fields ++ Map("id" -> Integer.valueOf(idx + 1))
        GenericRecord.create(schema).copy(fieldsWithId.asJava)
      }
      records.sorted(new IcebergRecordOrdering(sortOrder, schema)).map(_.getField("id").asInstanceOf[Int])
    }
  }

  def toTimestamp(time: String): java.lang.Long = {
    java.lang.Long.valueOf(Instant.parse(time).toEpochMilli * 1000)
  }

  def sortOrderBy(
      term: UnboundTerm[_],
      direction: SortDirection = SortDirection.ASC,
      nullOrder: NullOrder = NullOrder.NULLS_FIRST
  ): SortOrder = {
    SortOrder
      .builderFor(schema)
      .sortBy(term, direction, nullOrder)
      .build()
  }

  it("should respect sort direction") {
    val sortOrder = sortOrderBy(Expressions.ref("int"), SortDirection.DESC)

    val records = Records(
      Seq(
        Map("int" -> Integer.valueOf(1)),
        Map("int" -> Integer.valueOf(2)),
        Map("int" -> Integer.valueOf(3))
      )
    )

    records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(3, 2, 1)
  }

  it("should respect null order") {
    val sortOrder = sortOrderBy(Expressions.ref("int"), nullOrder = NullOrder.NULLS_LAST)

    val records = Records(
      Seq(
        Map("int" -> Integer.valueOf(5)),
        Map("int" -> null),
        Map("int" -> Integer.valueOf(1))
      )
    )

    records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(3, 1, 2)
  }

  it("should work correctly when ordering by multiple fields") {
    val sortOrder = SortOrder
      .builderFor(schema)
      .sortBy(Expressions.day("timestamp"), SortDirection.ASC, NullOrder.NULLS_FIRST)
      .sortBy(Expressions.truncate("string", 1), SortDirection.ASC, NullOrder.NULLS_FIRST)
      .sortBy(Expressions.ref("int"), SortDirection.DESC, NullOrder.NULLS_LAST)
      .build()

    val records = Records(
      Seq(
        Map("timestamp" -> toTimestamp("2024-01-01T19:00:00.000Z"), "string" -> "ab", "int" -> Integer.valueOf(1)),
        Map("timestamp" -> toTimestamp("2024-02-05T20:00:00.000Z"), "string" -> "ab", "int" -> Integer.valueOf(2)),
        Map("timestamp" -> toTimestamp("2024-01-01T23:00:00.000Z"), "string" -> "zb", "int" -> Integer.valueOf(3)),
        Map("timestamp" -> toTimestamp("2024-01-01T22:00:00.000Z"), "string" -> "ab", "int" -> null)
      )
    )

    records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(1, 4, 3, 2)
  }

  describe("primitive type ordering") {

    it("should work with int fields correctly") {
      val sortOrder = sortOrderBy(Expressions.ref("int"))

      val records = Records(
        Seq(
          Map("int" -> Integer.valueOf(4)),
          Map("int" -> Integer.valueOf(1)),
          Map("int" -> Integer.valueOf(5))
        )
      )

      records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(2, 1, 3)
    }

    it("should work with boolean fields correctly") {
      val sortOrder = sortOrderBy(Expressions.ref("boolean"))

      val records = Records(
        Seq(
          Map("boolean" -> Boolean.box(true)),
          Map("boolean" -> Boolean.box(false))
        )
      )

      records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(2, 1)
    }

    it("should work with string fields correctly") {
      val sortOrder = sortOrderBy(Expressions.ref("string"))

      val records = Records(
        Seq(
          Map("string" -> "xyz"),
          Map("string" -> "abc"),
          Map("string" -> "def")
        )
      )

      records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(2, 3, 1)
    }
  }

  describe("timestamp transformed field ordering") {
    val timestampRecords = Records(
      Seq(
        Map("timestamp" -> java.lang.Long.valueOf(Instant.parse("2024-04-24T00:53:33.000Z").toEpochMilli * 1000)),
        Map("timestamp" -> java.lang.Long.valueOf(Instant.parse("2023-02-12T23:23:53.000Z").toEpochMilli * 1000)),
        Map("timestamp" -> java.lang.Long.valueOf(Instant.parse("2023-01-19T12:43:52.000Z").toEpochMilli * 1000)),
        Map("timestamp" -> java.lang.Long.valueOf(Instant.parse("2021-11-29T22:45:11.000Z").toEpochMilli * 1000))
      )
    )

    val dateRecords = Records(
      Seq(
        Map("date" -> Integer.valueOf(LocalDate.of(2024, 1, 20).toEpochDay.toInt)),
        Map("date" -> Integer.valueOf(LocalDate.of(2022, 2, 19).toEpochDay.toInt)),
        Map("date" -> Integer.valueOf(LocalDate.of(2023, 4, 10).toEpochDay.toInt))
      )
    )

    it("should work with year(timestamp) correctly") {
      val sortOrder = sortOrderBy(Expressions.year("timestamp"))
      timestampRecords.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(4, 2, 3, 1)
    }

    it("should work with year(date) correctly") {
      val sortOrder = sortOrderBy(Expressions.year("date"))
      dateRecords.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(2, 3, 1)
    }

    it("should work with month(timestamp) correctly") {
      val sortOrder = sortOrderBy(Expressions.month("timestamp"))
      timestampRecords.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(4, 3, 2, 1)
    }

    it("should work with day(timestamp) correctly") {
      val sortOrder = sortOrderBy(Expressions.day("timestamp"))
      timestampRecords.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(4, 3, 2, 1)
    }

    it("should work with hour(timestamp) correctly") {
      val sortOrder = sortOrderBy(Expressions.hour("timestamp"))
      timestampRecords.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(4, 3, 2, 1)
    }
  }

  describe("truncate transformed field ordering") {
    it("should work with truncate(string) correctly") {
      val sortOrder = sortOrderBy(Expressions.truncate("string", 3))
      val records = Records(
        Seq(
          Map("string" -> "aazzzz"),
          Map("string" -> "aaazzz"),
          Map("string" -> "aaaaaa")
        )
      )

      records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(2, 3, 1)
    }

    it("should work with truncate(int) correctly") {
      val sortOrder = sortOrderBy(Expressions.truncate("int", 10))
      val records = Records(
        Seq(
          Map("int" -> Integer.valueOf(13)),
          Map("int" -> Integer.valueOf(2)),
          Map("int" -> Integer.valueOf(1))
        )
      )

      records.idsSortedBy(sortOrder) should contain theSameElementsInOrderAs Seq(2, 3, 1)
    }
  }
}
