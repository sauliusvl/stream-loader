package com.adform.streamloader.util

import com.adform.streamloader.util.UuidExtensions._
import com.google.cloud.bigquery.storage.v1.BigDecimalByteStringEncoder
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto
import com.google.protobuf.Descriptors.{Descriptor, FileDescriptor}
import com.google.protobuf.{ByteString, DescriptorProtos, DynamicMessage, Message}

import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.reflect.runtime.universe._

class ProtoConverter[P <: Product: TypeTag] {
  private val tpe = typeOf[P]
  private val tpeName = tpe.toString.split('.').last

  private def fields: Iterable[MethodSymbol] = tpe.decls.collect {
    case m: MethodSymbol if m.isCaseAccessor => m
  }

  lazy val descriptorProto: DescriptorProtos.DescriptorProto = {
    val builder = DescriptorProtos.DescriptorProto.newBuilder().setName(tpeName)

    fields.zipWithIndex.foreach { case (m, id) =>
      builder.addField(toProtoField(id + 1, m))
    }

    builder.build()
  }

  private def toProtoField(id: Int, m: MethodSymbol): FieldDescriptorProto = {
    val builder = FieldDescriptorProto.newBuilder()

    builder.setNumber(id)
    builder.setName(m.name.toString)

    m.returnType match {
      case t: TypeRef if t.typeSymbol == typeOf[Option[_]].typeSymbol =>
        builder.setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)
        builder.setType(toProtoType(t.args.head))
      case t: TypeRef if t.typeSymbol == typeOf[Array[_]].typeSymbol =>
        builder.setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
        builder.setType(toProtoType(t.args.head))
      case _ =>
        builder.setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED)
        builder.setType(toProtoType(m.returnType))
    }

    builder.build()
  }
  private def toProtoType(tp: Type): FieldDescriptorProto.Type = tp match {
    case t if t =:= typeOf[Int] => FieldDescriptorProto.Type.TYPE_INT32
    case t if t =:= typeOf[Long] => FieldDescriptorProto.Type.TYPE_INT64
    case t if t =:= typeOf[String] => FieldDescriptorProto.Type.TYPE_STRING
    case t if t =:= typeOf[LocalDateTime] => FieldDescriptorProto.Type.TYPE_INT64
    case t if t =:= typeOf[Double] => FieldDescriptorProto.Type.TYPE_DOUBLE
    case t if t =:= typeOf[Float] => FieldDescriptorProto.Type.TYPE_FLOAT
    case t if t =:= typeOf[Boolean] => FieldDescriptorProto.Type.TYPE_BOOL
    case t if t =:= typeOf[UUID] => FieldDescriptorProto.Type.TYPE_BYTES
    case t if t =:= typeOf[BigDecimal] => FieldDescriptorProto.Type.TYPE_BYTES
    case _ => throw new UnsupportedOperationException(s"Can't convert type $tp to proto")
  }

  private lazy val fileDescriptorProto = DescriptorProtos.FileDescriptorProto
    .newBuilder()
    .setName("dummy.proto")
    .addMessageType(descriptorProto)
    .build()

  private lazy val fileDescriptor: FileDescriptor =
    FileDescriptor.buildFrom(fileDescriptorProto, Array.empty[FileDescriptor])
  private lazy val descriptor: Descriptor = fileDescriptor.findMessageTypeByName(tpeName)

  def toProto(obj: P): Message = {
    val dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor)

    fields.zipWithIndex.foreach { case (m, id) =>
      val field = descriptor.findFieldByName(m.name.toString)
      val value = obj.productElement(id)

      value match {
        case Some(v) =>
          dynamicMessageBuilder.setField(field, toProtoValue(v))
        case None =>
        // do nothing
        case a: Array[_] =>
          a.foreach(v => dynamicMessageBuilder.addRepeatedField(field, toProtoValue(v)))
        case _ =>
          dynamicMessageBuilder.setField(field, toProtoValue(value))
      }
    }

    dynamicMessageBuilder.build()
  }

  private def toProtoValue(value: Any): Any = {
    value match {
      case dt: LocalDateTime =>
        val instant = dt.toInstant(ZoneOffset.UTC)
        val seconds = instant.getEpochSecond
        val nanos = instant.getNano
        (seconds * 1_000_000L) + (nanos / 1_000L)

      case uuid: UUID =>
        ByteString.copyFrom(uuid.toBytes)

      case d: BigDecimal =>
        BigDecimalByteStringEncoder.encodeToNumericByteString(d.bigDecimal)

      case _ => value
    }
  }
}
