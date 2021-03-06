package com.soundcloud.lsh
import scala.io.Source
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.io._

object Main {

  def main(args: Array[String]) {

    // init spark context
    val numPartitions = 8
    val conf = new SparkConf()
      .setAppName("LSH-Cosine")
      .setMaster("local[4]")
    val storageLevel = StorageLevel.MEMORY_AND_DISK
    val sc = new SparkContext(conf)

    // read in an example data set of word embeddings
    //preprocessing data
    val filename = "data/video_small_num.csv"
    val file = sc.textFile(filename)
    val header = file.first()
    val input = file.filter(row => row != header).map(r=>r.split(","))
    val user = input.map(r=>(r(0),r(1))).groupByKey().keys.collect()

    //generate index of products
    def truntoindex(item:(String,Array[String])):(String,Array[Double]) = {
      val indexofu = user.map(e=>if (item._2.contains(e)) 1.toDouble else 0.toDouble)
      (item._1,indexofu)
    }

    val inputdata = input.map(r => (r(1),Array(r(0)))).reduceByKey(_++_)
      .map(e=>(e._1,e._2)).map(truntoindex).collect()//(item,array(indexuser1,user2...))

    val data = sc.parallelize(inputdata, numPartitions).map {
      line =>
        //        val header = file.first()
        //        val input = file.filter(row => row != header).map(r=>r.split(","))
        //        val split = line.split(",")
        val word = line._1
        val features = line._2
        (word, features)
    }


    // create an unique id for each word by zipping with the RDD index
    val indexed = data.zipWithIndex.persist(storageLevel)

    // create indexed row matrix where every row represents one word
    val rows = indexed.map {
      case ((word, features), index) =>
        IndexedRow(index, Vectors.dense(features))
    }

    // store index for later re-mapping (index to word)
    val index = indexed.map {
      case ((word, features), index) =>
        (index, word)
    }.persist(storageLevel)

    // create an input matrix from all rows and run lsh on it
    val matrix = new IndexedRowMatrix(rows)
    val lsh = new Lsh(
      minCosineSimilarity = 0.5,
      dimensions = 50,
      numNeighbours = 200,
      numPermutations = 9,
      partitions = numPartitions,
      storageLevel = storageLevel
    )
    val similarityMatrix = lsh.join(matrix)

    // remap both ids back to words
    val remapFirst = similarityMatrix.entries.keyBy(_.i).join(index).values
    val remapSecond = remapFirst.keyBy { case (entry, word1) => entry.j }.join(index).values.map {
      case ((entry, word1), word2) =>
        (word1, word2, entry.value)
    }

    //calculate precision and recall
//    val groundtruth = "/data/video_small_ground_truth_cosine.csv"
//    val checkfile = Source.fromFile(groundtruth).getLines.map(r=>r.split(",").toSet).toArray.distinct
//    val tp = result.map(e=>Set(e._1,e._2))
//      .filter(checkfile.contains(_)).size
//
//    val precision = tp / result.length.toDouble //tp/tp+fp
//    val recall = tp / checkfile.length.toDouble
//
//    println("The tp is " + tp.toString)
//    println("The precision is " + precision.toString)
//    println("The recall is " + recall.toString)


    //define outputfilename
    var outputname = "data/Haoyu_Jiang_SimilarProducts_Cosine.txt"
    val pw = new PrintWriter(new File(outputname))
    remapSecond.map(r=>(r._1.toInt,r._2.toInt,r._3)).map(r=> if(r._1 < r._2) (r._1,r._2,r._3) else (r._2,r._1,r._3) ).sortBy{ case (x,y,z) => (x,y) }.collect()
      .map(e=>e._1 + "," + e._2 + "," + e._3.toString + "\n").foreach(pw.write)
    pw.close

    // print out the results for the first 10 words
    sc.stop()

  }
}
