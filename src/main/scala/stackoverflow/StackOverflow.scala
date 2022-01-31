package stackoverflow

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines: RDD[String] = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw: RDD[Posting] = rawPostings(lines)
    val grouped: RDD[(QID, Iterable[(Question, Answer)])] = groupedPostings(raw)
    val scored: RDD[(Question, HighScore)] = scoredPostings(grouped)
    val vectors: RDD[(LangIndex, HighScore)] = vectorPostings(scored)

    vectors.cache()
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means: Array[(Int, Int)] = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results: Array[(String, Double, LangIndex, LangIndex, LangIndex)] = clusterResults(means, vectors)

    vectors.unpersist()
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {
    val questions: RDD[(QID, Question)] = postings
      .filter((question: Posting) => question.postingType == 1)
      .map((question: Posting) => (question.id, question))

    val answers: RDD[(QID, Answer)] = postings
      .filter((answers: Posting) => answers.postingType == 2 && answers.parentId.isDefined)
      .map((answers: Posting) => (answers.parentId.get, answers))

    questions
      .join(answers)
      .groupByKey()
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }

    grouped
      .flatMap((idQuestionAnswer: (QID, Iterable[(Question, Answer)])) => idQuestionAnswer._2)
      .groupByKey()
      .map((pairQuestionAnswers: (Question, Iterable[Answer])) =>
        (pairQuestionAnswers._1, answerHighScore(pairQuestionAnswers._2.toArray)))
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }

    scored
      .map((questionScore: (Question, HighScore)) => (firstLangInTag(questionScore._1.tags, langs), questionScore._2))
      .filter((indexScore: (Option[Int], HighScore)) => indexScore._1.isDefined)
      .map((indexScore: (Option[LangIndex], HighScore)) => (indexScore._1.get * langSpread, indexScore._2))
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {

    val newMeans: Array[(Int, Int)] = means.clone()

    /**  Classifying the points & Setting the newMeans */
    vectors
      .map((vector: (LangIndex, HighScore)) => (findClosest(vector, means), vector))
      .groupByKey()
      .map((meanPoint: (Int, Iterable[(Int, HighScore)])) => (meanPoint._1, averageVectors(meanPoint._2)))
      .collect()
      .foreach((updatedMean: (Int, (LangIndex, HighScore))) => newMeans.update(updatedMean._1, updatedMean._2))

    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      if (debug) {
        println("Reached max iterations!")
      }
      newMeans
    }
  }


  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double): Boolean =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }


  /** Return the euclidean distance between two points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }


  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist: Double = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


/** Displaying results: */
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int, Int)] = {
    val closest: RDD[(Int, (LangIndex, HighScore))] = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped: RDD[(Int, Iterable[(LangIndex, HighScore)])] = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>

      val langLabel: String = langs(
        vs
        .groupBy(indexScore => indexScore._1)
        .mapValues(_.size)
        .maxBy(_._2)._1 / langSpread
      )

      val clusterSize: Int = vs.size

      val langPercent: Double = vs
        .count(indexScore => indexScore._1 == langs.indexOf(langLabel) * langSpread) / clusterSize * 100d

      val sortedScores: Seq[HighScore] = vs.map((indexScore: (LangIndex, HighScore)) => indexScore._2).toSeq.sorted
      val centerCluster = clusterSize / 2
      val medianScore: Int =
        if(clusterSize % 2 == 0) (sortedScores(centerCluster - 1) + sortedScores(centerCluster)) / 2
        else sortedScores(centerCluster)

      val maxScore: Int = vs.map(_._2).toList.max

      (langLabel, langPercent, clusterSize, medianScore, maxScore)
    }
    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  maxScore  medianScore  Dominant language (%percent)  Questions")
    println("================================================================")
    for ((lang, percent, size, median, max) <- results)
      println(f"${max}%10d  ${median}%11d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
