//package com.adform.streamloader
//
//import com.adform.streamloader.loaders.TestClickHouseRecord
//import com.adform.streamloader.model.{
//  ExampleMessage,
//  StreamPosition,
//  StreamRange,
//  StreamRangeBuilder,
//  StreamRecord,
//  Timestamp
//}
//
//import com.adform.streamloader.sink.batch.v2.stream.CsvStreamBatchBuilder
//import com.adform.streamloader.sink.batch.{
//  RecordBatch,
//  RecordBatcher,
//  RecordFormatter,
//  RecordPartitioner,
//  RecordStreamWriter
//}
//import com.adform.streamloader.sink.encoding.csv.CsvRecordEncoder
//import com.adform.streamloader.sink.file.{Compression, CountingOutputStream}
//import com.adform.streamloader.util.TimeProvider
//import com.github.luben.zstd.ZstdOutputStream
//import net.jpountz.lz4.LZ4BlockOutputStream
//import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
//import org.apache.kafka.common.TopicPartition
//import org.xerial.snappy.SnappyHadoopCompatibleOutputStream
//
//import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
//import java.time.Instant
//import java.util.zip.GZIPOutputStream
//import scala.collection.mutable
//
////
////class SortingBatchBuilder[R, B](
////  base: BatchBuilder[R, B],
////  sortOrder: Ordering[R]
////) extends BatchBuilder[R, B] {
////
////  override def add(record: R): Int = {
////    base.add(record)
////  }
////
////  override def build(): Option[B] = base.build()
////
////  override def discard(): Unit = base.discard()
////
////  override def estimateSizeBytes(): Long = base.estimateSizeBytes()
////
////  override def startTime: Instant = base.startTime
////
////  override def recordCount: Long = base.recordCount
////}
//
////
////
////abstract class StreamFileBuilder[-R, B](
////  recordStreamWriterFactory: OutputStream => RecordStreamWriter[R],
////  compression: Compression,
////  bufferSizeBytes: Int
////) extends BaseBatchBuilder[R, B] {
////
////  protected var isClosed = false
////
////  protected lazy val file: File = {
////    File.createTempFile("loader-", compression.fileExtension.map("." + _).getOrElse(""))
////  }
////
////  private val fileStream = new CountingOutputStream(new FileOutputStream(file))
////  private val compressedFileStream = compression match {
////    case Compression.NONE => new BufferedOutputStream(fileStream, bufferSizeBytes)
////    case Compression.ZSTD => new BufferedOutputStream(new ZstdOutputStream(fileStream), bufferSizeBytes)
////    case Compression.GZIP => new GZIPOutputStream(fileStream, bufferSizeBytes)
////    case Compression.BZIP => new BZip2CompressorOutputStream(fileStream)
////    case Compression.SNAPPY => new SnappyHadoopCompatibleOutputStream(fileStream, bufferSizeBytes)
////    case Compression.LZ4 => new LZ4BlockOutputStream(fileStream, bufferSizeBytes)
////  }
////
////  private val streamWriter = recordStreamWriterFactory(compressedFileStream)
////  streamWriter.writeHeader()
////
////  override def add(record: R): Int = {
////    streamWriter.writeRecord(record)
////    super.add(record)
////  }
////
////  override def estimateSizeBytes(): Long = fileStream.size
////
////  protected def buildFile(): Option[File] = {
////    if (!isClosed) {
////      streamWriter.writeFooter()
////      streamWriter.close()
////      isClosed = true
////      if (recordCount > 0) Some(file) else None
////    } else {
////      None
////    }
////  }
////
////  override def discard(): Unit = {
////    if (!isClosed) streamWriter.close()
////    if (file.exists()) file.delete()
////    isClosed = true
////  }
////}
////
//
//object Main {
//  def main(args: Array[String]): Unit = {
////
////    val recordFormatter: RecordFormatter[TestClickHouseRecord] = record => {
////      val msg = ExampleMessage.parseFrom(record.consumerRecord.value())
////      Seq(
////        TestClickHouseRecord(
////          record.consumerRecord.topic(),
////          record.consumerRecord.partition().toShort,
////          record.consumerRecord.offset(),
////          record.watermark,
////          msg.id,
////          msg.name,
////          // msg.timestamp,
////          msg.height,
////          msg.width,
////          msg.isEnabled,
////          msg.childIds,
////          msg.parentId,
////          msg.transactionId,
////          msg.moneySpent
////        )
////      )
////    }
////
////    val batcher = RecordBatcher
////      .builder()
////      .formatter(recordFormatter)
////      .batchBuilder(() => new ClickHouseCsvBatchBuilder[TestClickHouseRecord])
////      .build()
////
////    val builder = batcher.newBatchBuilder()
////    builder.add(StreamRecord(null, Timestamp(0)))
////    val batch = builder.build().get
//
//    case class Test(a: Int, b: String)
//    val file = new File("/tmp/test.csv")
////
////    class CsvFileBuilder(file: File) extends CsvStreamBatchBuilder[Test, File](new FileOutputStream(file)) {
////      override def toBatch: Option[File] = Some(file)
////    }
////
////    val b = new CsvFileBuilder(file)
////
////    b.add(Test(1, "aaa"))
////    b.add(Test(2, "cdef"))
////    b.add(Test(3, "ghi"))
////
////    println(b.build())
////    b.close()
//
//    println("hi")
//  }
//}
