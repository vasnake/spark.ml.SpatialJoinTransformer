package me.valik.toolbox

object StringToolbox {

  case class Separator(v: String) {
    override def toString: String = v
  }

  case class MapSeparators(pairs: Separator, kvs: Separator)

  object DefaultSeparators {
    implicit val oneSeparator = Separator(",")
    implicit val mapSeparators = MapSeparators(Separator(";"), Separator(":"))
  }

  implicit class RichString(val src: String) extends AnyVal {

    def splitTrim(implicit sep: Separator): Array[String] =
      src.trim.split("""\s*""" + sep + """\s*""").filterNot(_.isEmpty)

    def toList(implicit sep: Separator): Seq[String] =
      src.split(sep.v).map(_.trim).filter(_.nonEmpty)

    /**
      * Create Map from string, e.g. "foo: bar; poo: bazz"
      */
    def toMap(implicit sep: MapSeparators): Map[String, String] = {
      src.splitTrim(sep.pairs)
        .flatMap(kv => kv.splitTrim(sep.kvs) match {
          case Array(k, v) => Some(k -> v)
          case _ => None
        } ).toMap
    }

  }
}
