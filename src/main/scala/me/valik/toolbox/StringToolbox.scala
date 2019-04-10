package me.valik.toolbox

object StringToolbox {

  case class Separators(v: String, next: Option[Separators] = None)

  object DefaultSeparators {
    implicit val commaColon = Separators(",", Some(Separators(":")))
  }

  implicit class RichString(val src: String) extends AnyVal {

    /**
      * Convert string to array of trimmed strings, empty items will be filter out.
      * @param sep split marker
      * @return empty array or array of trimmed strings
      */
    def splitTrim(implicit sep: Separators): Array[String] =
      src.trim.split("""\s*""" + sep.v + """\s*""").filter(_.nonEmpty)

    /**
      * Convert string to Seq of trimmed strings, empty items will be filtered out.
      * Splitting don't use regexp, and result converted from array to sequence.
      * @param sep split marker
      * @return empty Seq or Seq of trimmed strings
      */
    def s2list(implicit sep: Separators): Seq[String] =
      src.split(sep.v).map(_.trim).filter(_.nonEmpty)

    /**
      * Create Map from string, e.g. "foo: bar; poo: bazz"
      */
    def parseMap(implicit sep: Separators): Map[String, String] = {
      val kvsep = sep.next.getOrElse(Separators(":"))
      val res = for {
        Array(k, v) <- src.splitTrim(sep).map(_.splitTrim(kvsep))
      } yield k -> v

      res.toMap
    }

  }
}
