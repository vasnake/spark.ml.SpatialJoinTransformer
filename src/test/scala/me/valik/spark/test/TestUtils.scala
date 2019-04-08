package me.valik.spark.test

import org.scalatest._
import org.apache.spark.sql.{Row, SparkSession}
import com.holdenkarau.spark.testing.SparkSessionProvider

trait SparkProvider {
  protected def loadSpark: SparkSession
  protected implicit lazy val spark: SparkSession = loadSpark
}

trait LocalSpark extends SparkProvider with BeforeAndAfterAll { this: Suite =>
  def loadSpark: SparkSession = SparkSessionProvider._sparkSession

  protected def sparkBuilder: SparkSession.Builder = SparkSession.builder()
    .master("local[*]")
    .appName(suiteName)
    .config("spark.ui.enabled", "false")
    .config("spark.app.id", suiteId)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    SparkSessionProvider._sparkSession = sparkBuilder.getOrCreate()
  }

  override protected def afterAll(): Unit = {
    SparkSessionProvider._sparkSession.stop()
    SparkSessionProvider._sparkSession = null
    super.afterAll()
  }
}

trait SimpleLocalSpark extends LocalSpark { this: Suite =>

  override def sparkBuilder: SparkSession.Builder = SparkSession.builder()
    .master("local[1]")
    .appName(suiteName)
    .config("spark.ui.enabled", "false")
    .config("spark.app.id", suiteId)
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.checkpoint.dir", "/tmp/checkpoints")
}

trait TestUtils {
  import java.io.File
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.{DataFrame, Dataset}

  def getResourceFilePath(fileName: String): String =
    new File(getClass.getClassLoader.getResource(fileName).toURI).getAbsolutePath

  def now = System.currentTimeMillis()

  def assertDataFrame(actualDF: DataFrame, expectedDF: DataFrame, strict: Boolean = true, select: Boolean = true): Unit = {
    val actual =
      if (select) {
        val fields = expectedDF.schema.fields.map(_.name)
        actualDF.select(fields.head, fields.tail: _*)
      }
      else actualDF

    if (strict) {
      if (!actual.schema.equals(expectedDF.schema)) {
        throw DatasetSchemaMismatch(schemaMismatchMessage(actual, expectedDF))
      }
    } else {
      if (!checkSchemasNotStrict(actual.schema, expectedDF.schema)) {
        throw DatasetSchemaMismatch(schemaMismatchMessage(actual, expectedDF))
      }
    }

    if (!actual.collect().sameElements(expectedDF.collect())) {
      throw DatasetContentMismatch(contentMismatchMessage(actual, expectedDF))
    }
  }

  case class DatasetSchemaMismatch(smth: String) extends Exception(smth)
  case class DatasetContentMismatch(smth: String) extends Exception(smth)

  private def schemaMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    s"""
Actual Schema:
${actualDS.schema}
Expected Schema:
${expectedDS.schema}
"""
  }

  private def contentMismatchMessage[T](actualDS: Dataset[T], expectedDS: Dataset[T]): String = {
    s"""
Actual DataFrame Content:
${DataFramePrettyPrint.showString(actualDS.toDF(), 20)}
Expected DataFrame Content:
${DataFramePrettyPrint.showString(expectedDS.toDF(), 20)}
"""
  }

  private def checkSchemasNotStrict(actualSchema: StructType, expectedSchema: StructType): Boolean = {
    val actual = actualSchema.copy(fields = actualSchema.fields.map(f => f.copy(metadata = null)))
    val expected = expectedSchema.copy(fields = expectedSchema.fields.map(f => f.copy(metadata = null)))
    actual.equals(expected)
  }

}

object DataFramePrettyPrint {

  import java.sql.Date
  import org.apache.commons.lang3.StringUtils
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.catalyst.util.DateTimeUtils

  def showString(df: DataFrame, numRows: Int = 0, truncate: Int = 80): String = {
    val takeResult = df.take(numRows + 1)
    val hasMoreData = takeResult.length > numRows
    val data = takeResult.take(numRows)

    // For array values, replace Seq and Array with square brackets
    // For cells that are beyond `truncate` characters, replace it with the
    // first `truncate-3` and "..."
    val rows: Seq[Seq[String]] = df.schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell =>
        val str = cell match {
          case null => "null"
          case binary: Array[Byte] => binary.map("%02X".format(_)).mkString("[", " ", "]")
          case array: Array[_] => array.mkString("[", ", ", "]")
          case seq: Seq[_] => seq.mkString("[", ", ", "]")
          case d: Date => DateTimeUtils.dateToString(DateTimeUtils.fromJavaDate(d))
          case _ => cell.toString
        }
        if (truncate > 0 && str.length > truncate) {
          // do not show ellipses for strings shorter than 4 characters.
          if (truncate < 4) str.substring(0, truncate)
          else str.substring(0, truncate - 3) + "..."
        } else str
      }: Seq[String]
    }

    val sb = new StringBuilder
    val numCols = df.schema.fieldNames.length

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(numCols)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex)
        colWidths(i) = cell.length.max(colWidths(i))
    }

    // Create SeparateLine
    val sep: String = colWidths.map("-" * _).addString(sb, "+", "+", "+\n").toString
    sb.append(sep)

    // names + data
    rows.zipWithIndex.map { case (row, rownum) => {
      if (rownum == 1) sb.append(sep)
      row.zipWithIndex.map { case (cell, i) =>
        if (truncate > 0) StringUtils.leftPad(cell.toString, colWidths(i))
        else StringUtils.rightPad(cell.toString, colWidths(i))
      }.addString(sb, "|", "|", "|\n")
    } }
    sb.append(sep)

    // For Data that has more than "numRows" records
    if (hasMoreData) {
      val rowsString = if (numRows == 1) "row" else "rows"
      sb.append(s"only showing top $numRows $rowsString\n")
    }

    sb.toString()
  }

}
