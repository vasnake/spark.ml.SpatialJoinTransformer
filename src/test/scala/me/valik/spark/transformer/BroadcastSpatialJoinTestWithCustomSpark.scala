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

package me.valik.spark.transformer

import org.scalatest._
import me.valik.spark.test._

class BroadcastSpatialJoinTestWithCustomSpark extends
  FlatSpec with Matchers with SimpleLocalSpark with DataFrameTestTools {

  import BroadcastSpatialJoinTest._

  // testOnly me.valik.spark.transformer.BroadcastSpatialJoinTestWithCustomSpark -- -z "smoke"
  it should "pass smoke test" in {
    import spark.implicits._

    val input = parsePointID(
      """
        |i1, 1, 1
        |i2, 2, 2
      """.stripMargin).toDS

    val data = parsePoiID(
      """
        |d1, 1.1, 1.1
        |d2, 2.1, 2.1
      """.stripMargin).toDS

    val expected = parsePointPoi(
      """
        |i1, 1, 1, d1
        |i2, 2, 2, d2
      """.stripMargin).toDS

    val res = makeTransformer(data.toDF).transform(input)

    //res.show(20, truncate=false)
    //res.collect should contain theSameElementsAs Seq(
    //  Row("i1", 1d, 1d, "d1"),
    //  Row("i2", 2d, 2d, "d2")
    //)

    assertDataFrameEquals(res, expected.selectPP)
  }
}
