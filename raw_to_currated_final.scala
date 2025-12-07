package com.newdevelopment.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileStatus, Path}
import org.yaml.snakeyaml.Yaml

import java.io.{FileInputStream, PrintWriter, StringWriter}
import java.net.URI
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Map => JMap, List => JList}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object RawToCurratedExcel {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: RawToCuratedFromYaml <configYamlPath>")
      System.exit(1)
    }
    val configPath = args(0)

    var spark: SparkSession = null
    var rawPath: String = null
    var errorBase: String = null

    try{
      // ----------------- load YAML & config -----------------
      val cfg = loadYaml(configPath)
      rawPath = cfg.getOrElse("raw_path", throw new IllegalArgumentException("raw_path missing")).asInstanceOf[String]
      val curatedBase = cfg.getOrElse("curated_base", throw new IllegalArgumentException("curated_base missing")).asInstanceOf[String]
      val archiveBase = cfg.getOrElse("archive_base", throw new IllegalArgumentException("archive_base missing")).asInstanceOf[String]
      errorBase = cfg.getOrElse("error_base", throw new IllegalArgumentException("error_base missing")).asInstanceOf[String]
      val fileGlob = cfg.getOrElse("file_glob", "*").asInstanceOf[String]
      val extensions: Seq[String] = cfg.get("extensions").map(_.asInstanceOf[JList[String]].asScala.map(_.toLowerCase)).getOrElse(Seq("csv","xlsx","xls"))
      val inferSchema = cfg.getOrElse("infer_schema", true).asInstanceOf[Boolean]
      val csvHeader = cfg.getOrElse("csv_header", true).asInstanceOf[Boolean]
      val csvMultiline = cfg.getOrElse("csv_multiline", false).asInstanceOf[Boolean]
      val csvEscape = cfg.getOrElse("csv_escape", "\"").asInstanceOf[String]
      val excelInfer = cfg.getOrElse("excel_infer_schema", false).asInstanceOf[Boolean]
      val timezone = cfg.getOrElse("timezone", "UTC").asInstanceOf[String]
      val writeFormat = cfg.getOrElse("write_format", "parquet").asInstanceOf[String]
      val writeModeRaw = cfg.getOrElse("write_mode", "overwrite").asInstanceOf[String]
      val archiveByDate = cfg.getOrElse("archive_partition_by_date", true).asInstanceOf[Boolean]

      // ----------------- Spark session -----------------
      spark = SparkSession
        .builder
        .appName("RawToCuratedFromYaml")
        .master("local[*]")
        .getOrCreate()

      // ----------------- main job logic (your original) -----------------
      val conf = new Configuration()
      val fs = FileSystem.get(new URI(rawPath), conf)

      // List files under rawPath (non-recursive). Filter by fileGlob substring and extensions.
      val rawFiles: Array[FileStatus] = fs.listStatus(new Path(rawPath))
        .filter(_.isFile)
        .filter { f =>
          val name = f.getPath.getName.toLowerCase
          (fileGlob == "*" || name.contains(fileGlob.toLowerCase)) &&
            extensions.exists(ext => name.endsWith(s".$ext"))
        }

      if (rawFiles.isEmpty) {
        println(s"No files found in $rawPath matching glob '$fileGlob' and extensions ${extensions.mkString(",")}")
      } else {
        rawFiles.foreach { fileStatus =>
          val filePath = fileStatus.getPath.toString
          try {
            processFile(
              spark = spark,
              filePath = filePath,
              curatedBase = curatedBase,
              archiveBase = archiveBase,
              errorBase = errorBase,
              inferSchema = inferSchema,
              csvHeader = csvHeader,
              csvMultiline = csvMultiline,
              csvEscape = csvEscape,
              excelInfer = excelInfer,
              timezone = timezone,
              writeFormat = writeFormat,
              writeModeRaw = writeModeRaw,
              archiveByDate = archiveByDate
            )
          } catch {
            case NonFatal(e) =>
              // Already handled inside processFile (error writes). Just print to driver logs.
              println(s"Processing failed for $filePath: ${e.getMessage}")
          }
        }
      }
    }catch {
      // 🔴 Any NonFatal error anywhere above comes here
      case NonFatal(e) =>
        if (spark != null && errorBase != null) {
          // Treat this as a job-level error; source_file can be rawPath or config
          writeErrorRecord(
            spark,
            errorBase,
            Option(rawPath).getOrElse(configPath),
            e
          )
        } else {
          // We don't know errorBase yet (e.g. YAML didn't parse), so just log to console
          e.printStackTrace()
        }

    } finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
  private def loadYaml(path: String): Map[String, Any] = {
    val yaml = new Yaml()
    val in = new FileInputStream(path)
    try {
      val data = yaml.load(in).asInstanceOf[JMap[String, Any]]
      if (data == null) Map.empty else data.asScala.toMap
    } finally {
      in.close()
    }
  }

  private def processFile(
                           spark: SparkSession,
                           filePath: String,
                           curatedBase: String,
                           archiveBase: String,
                           errorBase: String,
                           inferSchema: Boolean,
                           csvHeader: Boolean,
                           csvMultiline: Boolean,
                           csvEscape: String,
                           excelInfer: Boolean,
                           timezone: String,
                           writeFormat: String,
                           writeModeRaw: String,
                           archiveByDate: Boolean
                         ): Unit = {

    val conf = new Configuration()
    val fs = FileSystem.get(new URI(filePath), conf)
    val p = new Path(filePath)
    if (!fs.exists(p)) {
      writeErrorRecord(spark, errorBase, filePath, new RuntimeException(s"File not found: $filePath"))
      return
    }

    val fileName = p.getName
    val lower = fileName.toLowerCase
    val ext =
      if (lower.endsWith(".csv")) "csv"
      else if (lower.endsWith(".xlsx")) "xlsx"
      else if (lower.endsWith(".xls")) "xls"
      else ""

    val now = ZonedDateTime.ofInstant(Instant.now(), ZoneId.of(timezone))
    val datePart = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val timeStamp = now.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

    var df: DataFrame = null

    try {
      df = ext match {
        case "csv" =>
          spark.read
            .option("header", csvHeader.toString)
            .option("inferSchema", inferSchema.toString)
            .option("multiLine", csvMultiline.toString)
            .option("escape", csvEscape)
            .csv(filePath)

        case "xlsx" | "xls" =>
          spark.read
            .format("com.crealytics.spark.excel")
            .option("header", "true")
            .option("treatEmptyValuesAsNulls", "true")
            .option("inferSchema", excelInfer.toString)
            .load(filePath)

        case other =>
          throw new IllegalArgumentException(s"Unsupported extension: $other for file $fileName")
      }

      df.printSchema()

      // build curated target path. e.g. <curatedBase>/yyyyMMdd/<filename>_<ts>.parquet or .csv depending on writeFormat
      val extOut = writeFormat.toLowerCase match {
        case "parquet" => "parquet"
        case "csv"     => "csv"
        case other     => other
      }
      val curatedPath = s"${curatedBase.stripSuffix("/")}/${datePart}/${fileName}.${timeStamp}.${extOut}"

      val saveMode = writeModeRaw.toLowerCase match {
        case "overwrite" => SaveMode.Overwrite
        case "append"    => SaveMode.Append
        case _           => SaveMode.Overwrite
      }

      writeFormat.toLowerCase match {
        case "parquet" =>
          df.write.mode(saveMode).parquet(curatedPath)
        case "csv" =>
          df.write.mode(saveMode).option("header", "true").csv(curatedPath)
        case _ =>
          df.write.mode(saveMode).parquet(curatedPath) // fallback to parquet
      }

    } catch {
      case NonFatal(e) =>
        writeErrorRecord(spark, errorBase, filePath, e)
        // rethrow so outer loop logs it; process still attempts archive in finally
        throw e
    } finally {
      // archive: try rename; if rename fails, copy+delete. Use archiveByDate to partition
      val now = ZonedDateTime.ofInstant(Instant.now(), ZoneId.of(timezone))
      val datePart = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
      val timeStamp = now.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

      val archiveDestPathStr = if (archiveByDate) {
        s"${archiveBase.stripSuffix("/")}/${datePart}/${fileName}.${timeStamp}"
      } else {
        s"${archiveBase.stripSuffix("/")}/${fileName}.${timeStamp}"
      }
      val archiveDest = new Path(archiveDestPathStr)
      try {
        val parent = archiveDest.getParent
        if (!fs.exists(parent)) fs.mkdirs(parent)
        val moved = fs.rename(p, archiveDest)
        if (!moved) {
          org.apache.hadoop.fs.FileUtil.copy(fs, p, fs, archiveDest, false, conf)
          fs.delete(p, false)
        }
      } catch {
        case NonFatal(ex) =>
          writeErrorRecord(
            spark,
            errorBase,
            filePath,
            new RuntimeException(s"Archive failed to $archiveDestPathStr: ${ex.getMessage}", ex)
          )
      }
    }
  }

  private def writeErrorRecord(spark: SparkSession, errorBase: String, sourceFile: String, t: Throwable): Unit = {
    import spark.implicits._
    val now = ZonedDateTime.now()
    val ts = now.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)

    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    t.printStackTrace(pw)
    pw.flush()
    val stack = sw.toString

    val errDF = Seq((ts, sourceFile, t.getClass.getName, Option(t.getMessage).getOrElse(""), stack))
      .toDF("error_time", "source_file", "error_type", "error_message", "stack_trace")

    val conf = new Configuration()
    val fs = FileSystem.get(new URI(errorBase), conf)
    val datePart = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val errorTarget = s"${errorBase.stripSuffix("/")}/${datePart}/error_${System.currentTimeMillis()}.parquet"
    try {
      errDF.write.mode(SaveMode.Append).parquet(errorTarget)
    } catch {
      case NonFatal(e) =>
        // fallback: write plaintext
        try {
          val fallback = new Path(s"${errorBase.stripSuffix("/")}/fallback_error_${System.currentTimeMillis()}.txt")
          val out = fs.create(fallback)
          try {
            val text =
              s"""error_time: $ts
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
        } catch {
          case NonFatal(_) => // give up silently after fallback fails
        }
    }
  }
}
