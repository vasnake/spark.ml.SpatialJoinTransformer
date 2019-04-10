package me.valik.toolbox

import org.scalatest._

// happy path
class StringToolboxTest extends FlatSpec with Matchers {
  // testOnly me.valik.toolbox.StringToolboxTest

  //  splitTrim
  //  toList
  //  toMap
  import me.valik.toolbox.StringToolbox._

  ???
}

// corner cases

class StringToolboxTestSplitTrim extends FlatSpec with Matchers {
  import me.valik.toolbox.StringToolbox._
  import DefaultSeparators._

  // testOnly me.valik.toolbox.StringToolboxTestSplitTrim -- -z "empty"
  it should "produce empty array" in {
    val empty = Array.empty[String]

    assert("".splitTrim === empty)
    assert(",,,,,,, ,,,, , , , ".splitTrim === empty)
    assert(" , \t , \n \n \t ".splitTrim === empty)
  }

  it should "produce size 1 array" in {
    assert(".".splitTrim === Array("."))
    assert(" \t \n . \n \t ".splitTrim === Array("."))
    assert("foo,".splitTrim === Array("foo"))
    assert(",foo,".splitTrim === Array("foo"))
    assert("  foo  ,  ".splitTrim === Array("foo"))
    assert(" , foo ".splitTrim === Array("foo"))
    assert(" , foo , ".splitTrim === Array("foo"))
    assert(" ,,, foo ,, ".splitTrim === Array("foo"))
  }

  it should "process complex text" in {
    val text =
      """
        | The Mary,
        | had a little,
lamb.
      """.stripMargin
    assert(text.splitTrim === Array("The Mary", "had a little", "lamb."))
  }
}

class StringToolboxTestToList extends FlatSpec with Matchers {
  import me.valik.toolbox.StringToolbox.RichString
  import me.valik.toolbox.StringToolbox.DefaultSeparators.commaColon

  // testOnly me.valik.toolbox.StringToolboxTestToList -- -z "empty"
  it should "produce empty list" in {
    val empty = Seq.empty[String]

    assert("".list === empty)
    assert(",,,,,,, ,,,, , , , ".list === empty)
    assert(" , \t , \n \n \t ".list === empty)
  }

  it should "produce size 1 list" in {
    assert(".".list === Seq("."))
    assert(" \t \n . \n \t ".list === Seq("."))
    assert("foo,".list === Seq("foo"))
    assert(",foo,".list === Seq("foo"))
    assert("  foo  ,  ".list === Seq("foo"))
    assert(" , foo ".list === Seq("foo"))
    assert(" , foo , ".list === Seq("foo"))
    assert(" ,,, foo ,, ".list === Seq("foo"))
  }

  it should "process complex text" in {
    val text =
      """
        | The Mary,
        | had a little,
lamb.
      """.stripMargin
    assert(text.list === Seq("The Mary", "had a little", "lamb."))
  }
}


class StringToolboxTestToMap extends FlatSpec with Matchers {

  import me.valik.toolbox.StringToolbox.RichString
  import me.valik.toolbox.StringToolbox.DefaultSeparators.commaColon

  // testOnly me.valik.toolbox.StringToolboxTestToMap -- -z "empty"
  ???
}
