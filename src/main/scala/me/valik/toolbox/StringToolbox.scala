/*
 * Copyright 2019 Valentin Fedulov <vasnake@gmail.com>
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package me.valik.toolbox

import scala.util.Try

/**
  * Useful string operations, pimp-my-library style
  */
object StringToolbox {

  /**
    * String separators used in RichString class
    * @param v first level separator
    * @param next rest of the separators list
    */
  case class Separators(v: String, next: Option[Separators] = None)

  /**
    * Default implicit values for string separators.
    * Implicit string-to-separator conversion.
    */
  object DefaultSeparators {
    implicit val commaColon = Separators(",", Some(Separators(":")))

    import scala.language.implicitConversions
    implicit def stringToSeparators(sep: String): Separators = Separators(sep)
  }

  implicit class RichString(val src: String) extends AnyVal {

    /**
      * Split string by separator, take item in `pos` position and convert it to double
      * @param pos zero-based item position in the string
      * @param sep splitting marker
      * @return a parsed number or None
      */
    def extractNumber(pos: Int)(implicit  sep: Separators): Option[Double] = {
      Try { src.splitTrim(sep)(pos).toDouble }.toOption
    }

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
