package com.adform.streamloader.sink.batch.stream

import com.adform.streamloader.sink.batch.{BaseBatchBuilder, Compression}
import com.adform.streamloader.sink.encoding.csv.CsvRecordEncoder
import com.github.luben.zstd.ZstdOutputStream
import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import net.jpountz.lz4.LZ4BlockOutputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.xerial.snappy.SnappyHadoopCompatibleOutputStream

import java.io._
import java.util.zip.GZIPOutputStream

trait StreamBatch[B] {
  val out: OutputStream
  def build: B

  def map[O](f: B => O): StreamBatch[O] = {
    val parent = this
    new StreamBatch[O] {
      override val out: OutputStream = parent.out
      override def build: O = f(parent.build)
    }
  }
}

class LocalFileStreamBatch(file: File) extends StreamBatch[File] {
  override val out: OutputStream = new FileOutputStream(file)
  override def build: File = file
}

class TempFileStreamBatch extends StreamBatch[InputStream] {
  private val file = File.createTempFile("pref", "suff")
  override val out: OutputStream = new FileOutputStream(file)
  override def build: InputStream = new FileInputStream(file)
}

class BaseStreamBatchBuilder[-R, +B](
    stream: StreamBatch[B],
    compression: Compression,
    blockSize: Int
) extends BaseBatchBuilder[R, B] {

  private val countingStream: CountingOutputStream = new CountingOutputStream(stream.out)

  protected val out: OutputStream = compression match {
    case Compression.NONE => new BufferedOutputStream(countingStream, blockSize)
    case Compression.ZSTD => new BufferedOutputStream(new ZstdOutputStream(countingStream), blockSize)
    case Compression.GZIP => new GZIPOutputStream(countingStream, blockSize)
    case Compression.BZIP => new BZip2CompressorOutputStream(countingStream, blockSize)
    case Compression.SNAPPY => new SnappyHadoopCompatibleOutputStream(countingStream, blockSize)
    case Compression.LZ4 => new LZ4BlockOutputStream(countingStream, blockSize)
  }

  final override def estimateSizeBytes(): Long = countingStream.size

  override def build(): Option[B] = {
    out.flush()
    out.close()
    Some(stream.build)
  }

  def flush(): Unit = out.flush()

  override def close(): Unit = out.close()
}

case class CsvFormat(columnSeparator: String, rowSeparator: String, includeHeader: Boolean, nullValue: String)

object CsvFormat {
  val DEFAULT: CsvFormat = CsvFormat(columnSeparator = "\t", rowSeparator = "\n", includeHeader = false, nullValue = "")
}

class CsvStreamBatchBuilder[R: CsvRecordEncoder, +B](
    stream: StreamBatch[B],
    compression: Compression = Compression.NONE,
    bufferSizeBytes: Int = 4096,
    format: CsvFormat = CsvFormat.DEFAULT
) extends BaseStreamBatchBuilder[R, B](stream, compression, bufferSizeBytes) {

  private val encoder = implicitly[CsvRecordEncoder[R]]

  private val settings = new CsvWriterSettings()

  settings.setNullValue(format.nullValue)
  settings.getFormat.setDelimiter(format.columnSeparator)
  settings.getFormat.setLineSeparator(format.rowSeparator)

  private val writer = new CsvWriter(out, settings)

  if (format.includeHeader) {
    writer.writeHeaders(encoder.columnNames: _*)
  }

  override def add(record: R): Int = {
    writer.writeRow(encoder.safeEncode(record).map(_.orNull))
    super.add(record)
  }

  override def build(): Option[B] = {
    writer.flush()
    writer.close()
    super.build()
  }

  override def close(): Unit = {
    writer.close()
    super.close()
  }
}
