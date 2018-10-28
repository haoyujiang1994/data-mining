import org.apache.spark.{SparkConf, SparkContext}
import java.io._
import scala.io.Source

object JaccardLSH {

  val band = 10
  val totalhafun =20
  val row = totalhafun / band
  var modm = 2777


  def main(args: Array[String]) {

    val startTimeMillis = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("lsh_jaccard").setMaster("local")
    val sc = new SparkContext(conf)


    //preprocessing data
    val filename = args(0)
    val file = sc.textFile(filename)
    val header = file.first()
    val input = file.filter(row => row != header).map(r=>r.split(","))


    //data preprocessing
    //no need to collect for same key hash to same mapper in spark
    val products = input.map(r => (r(1),Array(r(0)))).reduceByKey(_++_)
      .map(e=>(e._1,e._2))   //(item,array(user1,user2...))
    val productmap = products.collect().toMap //for query candidates



    //define signature
    // for every item ==> generate 100 singature s.count = 100
    val signas = products.map(generatesig)   //(item1,(sigvalue1,sigvalue2...)) no need to groupby item
      .map(e=>(e._1,e._2.grouped(row).toArray)) // divide signatures into bands (item1,((s1..s25),(s26..s50)...))
      .flatMap(e =>e._2.map(r=>(e._1,r.mkString(","),e._2.indexWhere(_ sameElements r))))//add band number ((i1,s1,b0),(i3,s1,b1)...)
      .map(e=>(e._3,(e._2,e._1))) // ==>((b0,(s1,i1)),(b1,(s1,i3))...) s1.count = 25
      .collect()

    //signas.foreach(t=>println(t))

    //generate candidates
    val candidates = sc.parallelize(signas).groupByKey().values//group by band ((b0,((s1,i1),(s1,i2))...)=>((b0's(s1,i1),(s1,i2)),(),())
        .map(e=>e.groupBy(_._1).values.map(r=>r.map(v=>v._2)).filter(_.size > 1)//((b0's(s1,i1),(s1,i2)),(),())=>((b0's(s1,(i1,i2),(s2,(i1,i4),(),())
        .flatMap(k=>k.toList.sorted.combinations(2))) //get all the candidates in each band for same signatures
        .flatMap(p=>p.map(w=>(w.head,w(1),productmap(w.head).toSet.intersect(productmap(w(1)).toSet).size / productmap(w.head).toSet.union(productmap(w(1)).toSet).size.toDouble)))
      // toset union, array union is wrong, union != put two array together
        .filter(_._3>=0.5)//check candidate jaccard
        .collect().distinct
    //candidates.foreach(t=>println(t))



    //calculate precision and recall
//    val groundtruth = args(1)
//    val checkfile = Source.fromFile(groundtruth).getLines.map(r=>r.split(",").toSet).toArray.distinct
//    println(checkfile.length)
//    val tp = candidates.map(e=>Set(e._1,e._2))
//      .filter(checkfile.contains(_)).size
//
//    val precision = tp / candidates.length.toDouble //tp/tp+fp
//    val recall = tp / checkfile.length.toDouble
//
//    println("The tp is " + tp.toString)
//    println("The precision is " + precision.toString)
//    println("The recall is " + recall.toString)


    //define outputfilename
    var outputname = args(1)
    val pw = new PrintWriter(new File(outputname))
    candidates.map(r=>(r._1.toInt,r._2.toInt,r._3)).map(r=> if(r._1 < r._2) (r._1,r._2,r._3) else (r._2,r._1,r._3) ).sortBy{ case (x,y,z) => (x,y) }.map(e=>e._1 + "," + e._2 + "," + e._3.toString + "\n").foreach(pw.write)
    pw.close


    //timer
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.0
    println("The execution time is " + durationSeconds)
  }



  //(item1,array(user1,user2...))==>(item1,(sigvalue1,sigvalue2...))
  def generatesig(items: (String,Array[String])):(String,Array[Int]) = {
    var count = 0
    var signa= Array[Int]()
    var a = 3
    var b = 1
    while(count < totalhafun){
      val temp = items._2.map(e =>(e.toInt*a +b) %modm).sorted//sigvalue1,hash user directedly instead of matrix, smallest as siggnature
      // function in map input is element of rdd instead of rdd , nodeed collect==> decrease collect counts
      signa = temp(0) +: signa //(sigvalue1,sigvalue2...)
      a += 1
      b += 1
      count += 1
    }
    (items._1,signa)//(item1,(sigvalue1,sigvalue2...))

  }

}
