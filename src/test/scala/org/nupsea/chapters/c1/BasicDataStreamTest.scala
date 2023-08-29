package org.nupsea.chapters.c1

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class BasicDataStreamTest extends AnyFlatSpec with Matchers  {
  "A Set" should "have size 0 when created" in {
    Set.empty.size shouldEqual 0
  }

  it should "increase in size upon adding elements" in {
    val s = Set.empty + 1
    s.size shouldEqual 1
  }


}
