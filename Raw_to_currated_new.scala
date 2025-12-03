package com.sundogsoftware.spark.development.RawToCuratedJob

import com.sundogsoftware.spark.learningScala.RawToCuratedJob.{processFile, writeErrorRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.log4j._

import java.io.{PrintWriter, StringWriter}
import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.util.Try
import scala.util.control.NonFatal

object RawToCurated {
  def main(args: Array[String]): Unit = {
    val filePath = "data/EIMonitor.xlsx"
    val curatedBase = "data/curatedBase/"
    val archiveBase = "data/archiveBase/"
    val errorBase = "data/errorBase/"

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()
    println("spark session",spark)
    try {
      processFile(spark, filePath, curatedBase, archiveBase, errorBase)
    } finally {
      spark.stop()
    }
  }

  def processFile(spark: SparkSession,
                  filePath: String,
                  curatedBase: String,
                  archiveBase: String,
                  errorBase: String): Unit = {

    val conf = new Configuration()
    // If needed, set fs.defaultFS or kerberos configs here
    val fs = FileSystem.get(new URI(filePath), conf)

    val p = new Path(filePath)
    if (!fs.exists(p)) {
      val msg = s"Input file does not exist: $filePath"
      println(msg)
      writeErrorRecord(spark, errorBase, filePath, new RuntimeException(msg))
      throw new RuntimeException(msg)
    }

    val timeStamp = ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC"))
      .format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

    // extract filename and extension
    val fileName = p.getName

    // target curated path: curatedBase/<yyyyMMdd>/<filename-without-ext>_<timestamp>
    val curatedPath = s"${curatedBase.stripSuffix("/")}/${timeStamp.substring(0,8)}/${fileName}.${timeStamp}.parquet"


    val df: DataFrame = spark.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("treatEmptyValuesAsNulls", "true")
            .option("inferSchema", "false")
            .option("addColorColumns", "false")
            .load(filePath)

    // show schema for debugging - remove in production or change to logging
    df.printSchema()

    // Write to curated. Choose format as parquet (efficient) or csv depending on your needs
    try {
      df.write.mode(SaveMode.Append).parquet(curatedPath)
    }catch {
      case NonFatal(e) =>
        // write error details to error folder
        writeErrorRecord(spark, errorBase, filePath, e)
        // rethrow if desired or just log and continue. Here we rethrow so job exit status is non-zero.
        throw e
    }finally {
      // Always archive the file (move). If move fails, write an error to error folder too.
      val archiveDest = new Path(s"${archiveBase.stripSuffix("/")}/${timeStamp.substring(0,8)}/$fileName.$timeStamp")

      try {
        // create parent directories if not exists
        val parent = archiveDest.getParent
        if (!fs.exists(parent)) fs.mkdirs(parent)
        // move (rename) the file into archive; if rename across schemes fails, fallback to copy + delete
        val moved = fs.rename(p, archiveDest)
        if (!moved) {
          // fallback: copy then delete
          org.apache.hadoop.fs.FileUtil.copy(fs, p, fs, archiveDest, false, conf)
          fs.delete(p, false)
        }
      } catch {
        case NonFatal(ex) =>
          // Even archive error should be logged to error folder
          writeErrorRecord(spark, errorBase, filePath, new RuntimeException(s"Failed to archive file to ${archiveDest.toString}: ${ex.getMessage}", ex))
      }
    }
  }

  private def writeErrorRecord(spark: SparkSession, errorBase: String, sourceFile: String, t: Throwable): Unit = {
    import spark.implicits._
    val now = ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC"))
      .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

    // capture stack trace as string
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    t.printStackTrace(pw)
    pw.flush()
    val stack = sw.toString

    // build a small JSON-like structure
    val err = Seq((
      now,
      sourceFile,
      t.getClass.getName,
      Option(t.getMessage).getOrElse(""),
      stack
    )).toDF("error_time", "source_file", "error_type", "error_message", "stack_trace")

    // write one-file JSON / parquet / csv into the error folder with timestamp
    val timeStamp = now.replaceAll("[:\\-]", "").replaceAll("\\.", "")
    val target = s"${errorBase.stripSuffix("/")}/$timeStamp/error_${(System.currentTimeMillis()).toString}.error"

    Try {
      err.write.mode(SaveMode.Append).csv(target)
    }.recover {
      case NonFatal(e) =>
        // If writing via Spark fails (rare), try writing small text file via Hadoop FS
        val conf = new Configuration()
        val fs = FileSystem.get(new URI(errorBase), conf)
        val fallbackPath = new Path(s"${errorBase.stripSuffix("/")}/fallback_error_${System.currentTimeMillis()}.txt")
        val out = fs.create(fallbackPath)
        try {
          val text =
            s"""error_time: $now
               |source_file: $sourceFile
               |error_type: ${t.getClass.getName}
               |error_message: ${Option(t.getMessage).getOrElse("")}
               |stack_trace:
               |$stack
               |""".stripMargin
          out.write(text.getBytes("UTF-8"))
        } finally {
          out.close()
        }
    }
  }
}
