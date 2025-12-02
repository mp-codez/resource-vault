package com.sundogsoftware.spark.learningScala

import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.SparkException
import scala.util.control.NonFatal
import scala.util.Try
import java.io.PrintWriter
import java.io.StringWriter

object RawToCuratedJob {
  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println("Usage: RawToCuratedJob <filePath> <rawBase> <curatedBase> <archiveBase> <errorBase>")
      System.exit(1)
    }

    val filePath = args(0)            // full hdfs/s3 path to file in raw layer, e.g. hdfs://nn:8020/data/raw/myfile.csv
    val rawBase = args(1)            // base raw folder (not strictly necessary but kept for clarity)
    val curatedBase = args(2)        // e.g. hdfs://nn:8020/data/curated/
    val archiveBase = args(3)        // e.g. hdfs://nn:8020/data/archive/
    val errorBase = args(4)          // e.g. hdfs://nn:8020/data/error/

    val spark = SparkSession.builder()
      .appName("RawToCuratedJob")
      .getOrCreate()

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
      writeErrorRecord(spark, errorBase, filePath, new RuntimeException(msg))
      throw new RuntimeException(msg)
    }

    // extract filename and extension
    val fileName = p.getName
    val ext = fileName.toLowerCase match {
      case n if n.endsWith(".csv")  => "csv"
      case n if n.endsWith(".xlsx") => "xlsx"
      case n if n.endsWith(".xls")  => "xls"
      case other                    => other.split('.').lastOption.getOrElse("")
    }

    val timeStamp = ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("UTC"))
      .format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

    // target curated path: curatedBase/<yyyyMMdd>/<filename-without-ext>_<timestamp>
    val curatedPath = s"${curatedBase.stripSuffix("/")}/${timeStamp.substring(0,8)}/${fileName}.${timeStamp}.parquet"

    var df: DataFrame = null

    try {
      df = ext match {
        case "csv" =>
          spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")       // optional, adjust as needed
            .option("escape", "\"")
            .csv(filePath)

        case "xlsx" | "xls" =>
          // Requires com.crealytics:spark-excel
          spark.read
            .format("com.crealytics.spark.excel")
            .option("useHeader", "true")
            .option("treatEmptyValuesAsNulls", "true")
            .option("inferSchema", "false") // set true if you want infer
            .option("addColorColumns", "false")
            // default sheet or specify .option("sheetName","Sheet1")
            .load(filePath)

        case other =>
          throw new IllegalArgumentException(s"Unsupported file extension: $other")
      }

      // show schema for debugging - remove in production or change to logging
      df.printSchema()

      // Write to curated. Choose format as parquet (efficient) or csv depending on your needs
      df.write.mode(SaveMode.Overwrite).parquet(curatedPath)

    } catch {
      case NonFatal(e) =>
        // write error details to error folder
        writeErrorRecord(spark, errorBase, filePath, e)
        // rethrow if desired or just log and continue. Here we rethrow so job exit status is non-zero.
        throw e
    } finally {
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

    // build a small JSON-like structure (you can make it real JSON)
    val err = Seq((
      now,
      sourceFile,
      t.getClass.getName,
      Option(t.getMessage).getOrElse(""),
      stack
    )).toDF("error_time", "source_file", "error_type", "error_message", "stack_trace")

    // write one-file JSON / parquet / csv into the error folder with timestamp
    val timeStamp = now.replaceAll("[:\\-]", "").replaceAll("\\.", "")
    val target = s"${errorBase.stripSuffix("/")}/$timeStamp/error_${(System.currentTimeMillis()).toString}.parquet"

    Try {
      err.write.mode(SaveMode.Append).parquet(target)
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
