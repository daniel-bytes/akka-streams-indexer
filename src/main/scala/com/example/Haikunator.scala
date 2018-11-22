package com.example

// See https://kernelgarden.wordpress.com/2014/06/27/a-heroku-like-name-generator-in-scala/
object Haikunator {
  import scala.util.Random.nextInt

  // full dictionaries at https://github.com/bmarcot/haiku
  val adjs = List("autumn", "hidden", "bitter", "misty", "silent",
    "reckless", "daunting", "short", "rising", "strong", "timber", "tumbling",
    "silver", "dusty", "celestial", "cosmic", "crescent", "double", "far",
    "terrestrial", "huge", "deep", "epic", "titanic", "mighty", "powerful")

  val nouns = List("waterfall", "river", "breeze", "moon", "rain",
    "wind", "sea", "morning", "snow", "lake", "sunset", "pine", "shadow", "leaf",
    "sequoia", "cedar", "wrath", "blessing", "spirit", "nova", "storm", "burst",
    "giant", "elemental", "throne", "game", "weed", "stone", "apogee", "bang")

  def getRandElt[A](xs: List[A]): A = xs.apply(nextInt(xs.size))

  def getRandNumber(ra: Range): String = {
    (ra.head + nextInt(ra.end - ra.head)).toString
  }

  def haiku: String = {
    val xs = getRandNumber(1000 to 9999) :: List(nouns, adjs).map(getRandElt)
    xs.reverse.mkString("-")
  }
}
