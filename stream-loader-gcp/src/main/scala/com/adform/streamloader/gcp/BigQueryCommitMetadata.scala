package com.adform.streamloader.gcp

import org.json4s._
import org.json4s.native.JsonMethods._

import com.adform.streamloader.model.Timestamp

case class BigQueryCommitMetadata(watermark: Timestamp, streamName: Option[String], streamOffset: Option[Long]) {
  def toJson: String = {
    Seq(
      Some("watermark" -> watermark.millis.toString),
      streamName.map(n => "stream_name" -> s"\"$n\""),
      streamOffset.map(o => "stream_offset" -> o.toString)
    ).collect {
      case Some((key, value)) => s"$key: $value"
    }.mkString("{", ",", "}")
  }
}

object BigQueryCommitMetadata {
  def parseJson(str: String): BigQueryCommitMetadata = {
    val props = (parse(str) match {
      case JObject(kvs) => kvs
      case _ => throw new IllegalArgumentException(s"Invalid Kafka commit metadata: $str")
    }).toMap

    BigQueryCommitMetadata(
      Timestamp(getLong(props("watermark"))),
      props.get("stream_name").map(_.asInstanceOf[JString].s),
      props.get("stream_offset").map(getLong)
    )
  }

  private def getLong(value: JValue): Long = value match {
    case JLong(l) => l
    case JInt(n) => n.toLong
    case _ => throw new IllegalArgumentException(s"Expected $value to be numeric")
  }
}
