package com.adform.streamloader.hadoop

import com.adform.streamloader.hadoop.parquet.ParquetConfig
import com.adform.streamloader.model.StreamRange
import com.adform.streamloader.sink.batch.v2.{BaseBatchBuilder, RecordBatch}
import com.adform.streamloader.sink.batch.v2.stream.{BaseStreamBatchBuilder, StreamBatch}
import com.adform.streamloader.sink.file.Compression.NONE
import org.apache.hadoop.fs.{FileSystem, Path}
import com.adform.streamloader.util.UuidExtensions.randomUUIDv7
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.OutputFile
import java.io.File
import java.util.UUID
import com.sksamuel.avro4s._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopOutputFile

import java.io.OutputStream

//class HadoopIO(fs: FileSystem, stagingDirectory: Path, nameFormat: String) extends StreamBatch[Path] {
//
//  def this(fs: FileSystem, stagingDirectory: String, nameFormat: String = "%s.tmp") = this(fs, new Path(stagingDirectory), nameFormat)
//
//  val path = new Path(stagingDirectory, new Path(nameFormat.format(randomUUIDv7().toString)))
//
//  override val out: OutputStream = {
//    println(s"Creating $path ...")
//    fs.create(path)
//  }
//
//  val outputFile = HadoopOutputFile.fromPath(path, fs.getConf)
//
//  override def build: Path = path
//}

// case class HadoopFileBatch(path: Path, recordCount: Long, recordRanges: Seq[StreamRange]) extends RecordBatch

///////////////////////////////////////////////////////////////////////////////////////

/**
  * Base class for Avro based parquet file builders.
  *
  * @param schema Avro schema to use.
  * @param config Parquet file configuration.
  * @tparam R type of the records being added.
  */
abstract class AvroParquetFileBuilder[R](file: OutputFile, schema: Schema, config: ParquetConfig = ParquetConfig())
    extends BaseBatchBuilder[R, String] {

  protected val parquetWriter: ParquetWriter[GenericRecord] = {
    val conf = new Configuration()
    val builder = AvroParquetWriter
      .builder[GenericRecord](file)
      .withSchema(schema)
      .withConf(conf)
    config.applyTo(builder)
    builder.build()
  }

  override def estimateSizeBytes(): Long = parquetWriter.getDataSize

  override def build(): Option[String] = {
    parquetWriter.close()
    Some(file.getPath)
    // Some(io.path)
  }

  override def close(): Unit = {
    parquetWriter.close()
  }
}

/**
  * Parquet writer that derives the Avro schema and encoder at compile time from the record type.
  */
class DerivedAvroParquetFileBuilder[R: Encoder: Decoder: SchemaFor](
    file: OutputFile,
    config: ParquetConfig = ParquetConfig()
) extends AvroParquetFileBuilder[R](file, AvroSchema[R], config) {

  private val recordFormat = RecordFormat[R]

  override def add(record: R): Int = {
    parquetWriter.write(recordFormat.to(record))
    super.add(record)
  }
}
