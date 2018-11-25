package com.example

// See https://kernelgarden.wordpress.com/2014/06/27/a-heroku-like-name-generator-in-scala/
object Haikunator {
  import scala.util.Random.nextInt

  val adjs = List("autumn", "hidden", "bitter", "misty", "silent",
    "reckless", "daunting", "short", "rising", "strong", "timber", "tumbling",
    "silver", "dusty", "celestial", "cosmic", "crescent", "double", "far",
    "terrestrial", "huge", "deep", "epic", "titanic", "mighty", "powerful")

  val nouns = List("waterfall", "river", "breeze", "moon", "rain",
    "wind", "sea", "morning", "snow", "lake", "sunset", "pine", "shadow", "leaf",
    "sequoia", "cedar", "wrath", "blessing", "spirit", "nova", "storm", "burst",
    "giant", "elemental", "throne", "game", "weed", "stone", "apogee", "bang")

  /**
    * Creates a new random "haiku" string
    */
  def haiku: String = List(adjs, nouns).map(getRandElt).mkString("-")

  private def getRandElt[A](xs: List[A]): A = xs.apply(nextInt(xs.size))
}
