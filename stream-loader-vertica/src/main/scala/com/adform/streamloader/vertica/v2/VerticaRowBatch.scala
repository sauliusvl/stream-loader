package com.adform.streamloader.vertica.v2

import com.adform.streamloader.sink.batch.v2.stream.{BaseStreamBatchBuilder, CsvStreamBatchBuilder, StreamBatch}
import com.adform.streamloader.sink.encoding.csv.{CsvFormat, CsvRecordEncoder}
import com.adform.streamloader.sink.file.Compression
import com.adform.streamloader.sink.file.Compression.NONE
import com.adform.streamloader.vertica.VerticaLoadMethod
import com.adform.streamloader.vertica.file.native.{NativeVerticaRecordEncoder, NativeVerticaRecordStreamWriter}

import java.io.InputStream

trait VerticaRowBatch {
  def inputStream: InputStream
  def copyStatement(table: String): String
}

trait ExternalVerticaRowBatch extends VerticaRowBatch {
  val id: Long
}

case class VerticaCsvRowBatch(
    inputStream: InputStream,
    loadMethod: VerticaLoadMethod,
    compression: Compression,
    format: CsvFormat
) extends VerticaRowBatch {
  override def copyStatement(table: String): String = {
    val skipHeader = if (format.includeHeader) "SKIP 1" else ""
    val delimiter = format.columnSeparator match {
      case "\t" => "E'\\t'"
      case x => s"'$x'"
    }
    s"COPY $table FROM STDIN ${VerticaRowBatch.compressionStr(compression)} DELIMITER $delimiter $skipHeader" +
      s" ABORT ON ERROR ${VerticaRowBatch.loadMethodStr(loadMethod)} NO COMMIT"
  }
}

case class VerticaNativeRowBatch(
    inputStream: InputStream,
    compression: Compression,
    loadMethod: VerticaLoadMethod
) extends VerticaRowBatch {
  override def copyStatement(table: String): String = {
    s"COPY $table FROM STDIN ${VerticaRowBatch.compressionStr(compression)} NATIVE ABORT ON ERROR ${VerticaRowBatch.loadMethodStr(loadMethod)} NO COMMIT"
  }
}

class VerticaCsvRowBatchBuilder[R: CsvRecordEncoder](
    stream: StreamBatch[InputStream],
    loadMethod: VerticaLoadMethod,
    compression: Compression = NONE
) extends CsvStreamBatchBuilder[R, VerticaRowBatch](
      stream.map(i => VerticaCsvRowBatch(i, loadMethod, compression, CsvFormat.DEFAULT))
    )

class VerticaNativeRowBatchBuilder[R: NativeVerticaRecordEncoder](
    stream: StreamBatch[InputStream],
    loadMethod: VerticaLoadMethod,
    compression: Compression = NONE
) extends BaseStreamBatchBuilder[R, VerticaRowBatch](
      stream.map(i => VerticaNativeRowBatch(i, compression, loadMethod)),
      compression,
      blockSize = 1
    ) {

  private val writer = new NativeVerticaRecordStreamWriter[R](out)

  writer.writeHeader()

  override def add(record: R): Int = {
    writer.writeRecord(record)
    super.add(record)
  }
}

object VerticaRowBatch {
  def compressionStr(compression: Compression): String = compression match {
    case Compression.NONE => ""
    case Compression.ZSTD => "ZSTD"
    case Compression.GZIP => "GZIP"
    case Compression.BZIP => "BZIP"
    case _ => throw new UnsupportedOperationException(s"Compression $compression is not supported in Vertica")
  }

  def loadMethodStr(loadMethod: VerticaLoadMethod): String = loadMethod match {
    case VerticaLoadMethod.AUTO => "AUTO"
    case VerticaLoadMethod.DIRECT => "DIRECT"
    case VerticaLoadMethod.TRICKLE => "TRICKLE"
  }
}
