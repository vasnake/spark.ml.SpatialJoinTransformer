package me.valik.spark.test

import org.scalatest._
import org.apache.spark.sql.{Row, SparkSession}
import com.holdenkarau.spark.testing.SparkSessionProvider

import scala.util.Try

trait SparkProvider {
  protected def loadSpark(): SparkSession
  @transient protected implicit lazy val spark: SparkSession = loadSpark()
}

trait LocalSpark extends SparkProvider with BeforeAndAfterAll { this: Suite =>
  def loadSpark(): SparkSession = SparkSessionProvider._sparkSession

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

trait DataFrameTestTools {
  import java.io.File
  import org.apache.spark.sql.types.StructType
  import org.apache.spark.sql.DataFrame
  import me.valik.spark.test.DataFrameTestTools._

  def getResourceFilePath(fileName: String): String =
    new File(getClass.getClassLoader.getResource(fileName).toURI).getAbsolutePath

  def now: Long = System.currentTimeMillis()

  def assertDataFrameEqualsStrict(actualDF: DataFrame, expectedDF: DataFrame, select: Boolean = true): Unit =
    assertDFEquals(actualDF, expectedDF, strict = true, select)

  def assertDataFrameEquals(actualDF: DataFrame, expectedDF: DataFrame, select: Boolean = true): Unit =
    assertDFEquals(actualDF, expectedDF, strict = false, select)

  private def assertDFEquals(actualDF: DataFrame, expectedDF: DataFrame,
    strict: Boolean, select: Boolean): Unit = {

    val actual =
      if (select) {
        val fields = expectedDF.schema.fields.map(_.name)
        actualDF.select(fields.head, fields.tail: _*)
      }
      else actualDF

    if ((strict && !actual.schema.equals(expectedDF.schema)) ||
      (!strict && !checkSchemasNotStrict(actual.schema, expectedDF.schema))
    ) throw DatasetSchemaMismatch(schemaMismatchMessage(actual, expectedDF))

    if (!actual.collect.sameElements(expectedDF.collect))
      throw DatasetContentMismatch(contentMismatchMessage(actual, expectedDF))
  }

  private def schemaMismatchMessage(actualDS: DataFrame, expectedDS: DataFrame): String = {
    s"""not schema.equals
Actual Schema:
${actualDS.schema}
Expected Schema:
${expectedDS.schema}
"""
  }

  private def contentMismatchMessage(actualDS: DataFrame, expectedDS: DataFrame): String = {
    s"""not sameElements
Actual DataFrame Content:
${DataFramePrettyPrint.toString(actualDS.toDF(), 20)}
Expected DataFrame Content:
${DataFramePrettyPrint.toString(expectedDS.toDF(), 20)}
"""
  }

  private def checkSchemasNotStrict(actualSchema: StructType, expectedSchema: StructType): Boolean = {
    val actual = actualSchema.copy(fields = actualSchema.fields.map(f => f.copy(metadata = null)))
    val expected = expectedSchema.copy(fields = expectedSchema.fields.map(f => f.copy(metadata = null)))
    actual.equals(expected)
  }

}

object DataFrameTestTools {
  case class DatasetSchemaMismatch(msg: String) extends Exception(msg)
  case class DatasetContentMismatch(msg: String) extends Exception(msg)
}

object DataFramePrettyPrint {

  import java.sql.Date
  import org.apache.commons.lang3.StringUtils
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.catalyst.util.DateTimeUtils

  def cell2String(cell: Any, truncate: Int): String = {
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
  }

  def toString(df: DataFrame, numRows: Int = 0, cellMaxLen: Int = 20): String = {
    val (hasMoreData, data) = {
      val plusOne = df.take(numRows + 1)
      (plusOne.length > numRows, plusOne.take(numRows))
    }

    val rows: Seq[Seq[String]] = df.schema.fieldNames.toSeq +: data.map { row =>
      row.toSeq.map { cell2String(_, cellMaxLen) }
    }

    // Initialise the width of each column to a minimum value of '3'
    val colWidths = Array.fill(df.schema.fieldNames.length)(3)

    // Compute the width of each column
    for (row <- rows) {
      for ((cell, i) <- row.zipWithIndex)
        colWidths(i) = cell.length.max(colWidths(i))
    }

    val res = new StringBuilder

    // draw top line
    val sep: String = colWidths.map("-" * _).addString(res, "+", "+", "+\n").toString

    // names + data
    rows.zipWithIndex.foreach { case (row, rownum) => {
      // underline table header: colnames
      if (rownum == 1) res.append(sep)
      // data rows
      row.zipWithIndex.map { case (cell, i) =>
        if (cellMaxLen > 0) StringUtils.leftPad(cell.toString, colWidths(i))
        else StringUtils.rightPad(cell.toString, colWidths(i))
      }.addString(res, "|", "|", "|\n")
    } }
    // last line
    res.append(sep)

    // For Data that has more than "numRows" records
    if (hasMoreData)
      res.append(s"only showing top $numRows ${if (numRows == 1) "row" else "rows"}\n")

    res.toString()
  }

}
