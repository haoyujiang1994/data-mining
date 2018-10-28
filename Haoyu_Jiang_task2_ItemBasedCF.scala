import org.apache.spark.{SparkConf, SparkContext}
import java.io._
import scala.io.Source

object ItemBasedCF {

  val band = 10
  val totalhafun =20
  val row = totalhafun / band
  var modm = 2777


  def main(args: Array[String]) {

    val startTimeMillis = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("lsh_jaccard").setMaster("local")
    val sc = new SparkContext(conf)


    //preprocessing data

    val filenamewhole = args(0)
    val filenametest = args(1)
    val wholefile = sc.textFile(filenamewhole)
    val testfile = sc.textFile(filenametest)
    val headerwhole = wholefile.first()
    val headertest = testfile.first()

    val origin =  wholefile.filter(row => row != headerwhole).map(r=>r.split(","))
    val whole = origin.map(r=>(r(0),r(1),r(2).toDouble)).map(r=>((r._1,r._2),r._3))//.collect()
    val test = testfile.filter(row => row != headertest).map(r=>r.split(",")).map(r=>(r(0),r(1),r(2).toDouble)).map(r=>((r._1,r._2),r._3))//.collect()//user,item,rating
    val interva =  whole.subtractByKey(test)
    val train = interva.map(r=>(r._1._2,(r._1._1,r._2))).groupByKey().collect()//.map(r=>(r._1,r._2.map(e=>e._2)))//item,((user,rating)...)
    val trainrdd = interva.map(r => (r._1._1,(r._1._2,r._2))).groupByKey().collect()

    //data preprocessing
    //no need to collect for same key hash to same mapper in spark
    val products = interva.map(r=>(r._1._1,r._1._2,r._2)).map(r => (r._2,Array(r._1))).reduceByKey(_++_)
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

    val candidates_temp = candidates.map(r=> if(r._1 < r._2) (r._1,r._2,r._3) else (r._2,r._1,r._3) ).sortBy{ case (x,y,z) => (x,y) }
      .map(r=>(r._1,r._2))//.groupBy(_._1) //map(r=>(r._1,r._2.map(e=>e._2)))

    val candidateshalf1 = candidates_temp.groupBy(_._1)
    val candidateshalf2 = candidates_temp.groupBy(_._1)
    val candidatesmap = candidateshalf1 ++ candidateshalf2

    def pearson(user_item:(String,String)) : ((String,String),Double) = {

      var relative_item_record = Array[(String, Array[(String, Double)])]() //.map(e=>(e._1,e._2.toArray))

      if (candidatesmap.keys.toArray.contains(user_item._2)) {
        val relative_item_lsh = candidatesmap(user_item._2).map(e => e._2)
        for (record <- train) {
          //record item,user,rating
          if (record._2.map(r => r._1).toArray.contains(user_item._1) && relative_item_lsh.contains(record._1) && record._1 != user_item._2) {
            relative_item_record = (record._1, record._2.toArray) +: relative_item_record
          }
        }
      } else {
        for (record <- train) {
          //record item,user,rating
          if (record._2.map(r => r._1).toArray.contains(user_item._1)&& record._1 != user_item._2) {
            relative_item_record = (record._1, record._2.toArray) +: relative_item_record
          }
        }
      }


        val target_record = train.filter(_._1 == user_item._2)(0)
        val ave = trainrdd.filter(_._1 == user_item._1).flatMap(e => e._2.map(r => r._2))

        if (relative_item_record.length == 0) {
          return ((user_item._1, user_item._2), ave.sum/ave.length)
        }


        def formula_Coefficient(item_rating: (String, Array[(String, Double)])): (String, Double, Double) = {
          //item,user,rating

          val target_userset = target_record._2.toArray.map(r => r._1)
          val relative_user_userset = item_rating._2.map(e => e._1)
          val shared_item = target_userset.intersect(relative_user_userset)

          if (shared_item.length == 0) {
            return (item_rating._1, 0.0, 0.0)
          }

          val target_rating = target_record._2.filter(r => shared_item.contains(r._1)).toArray.sortBy(_._1).map(r => r._2.toDouble)
          val target_rating_avg = target_record._2.map(e => e._2.toDouble).sum / target_record._2.size.toDouble

          val relative_user_rating = item_rating._2.filter(r => shared_item.contains(r._1)).sortBy(_._1).map(r => r._2.toDouble)
          val relative_user_rating_avg = item_rating._2.map(e => e._2.toDouble).sum / item_rating._2.length.toDouble


          val Denominator_target = math.pow(target_rating.map(r => math.pow(r - target_rating_avg, 2)).sum, 0.5)
          val Denominator_relative = math.pow(relative_user_rating.map(r => math.pow(r - relative_user_rating_avg, 2)).sum, 0.5)
          var Coefficient = target_rating.map(r => r - target_rating_avg).zip(relative_user_rating.map(r => r - relative_user_rating_avg))
            .map(e => e._1 * e._2).sum / Denominator_relative * Denominator_target //average on co_reated items

          if (Coefficient.isNaN) {
            Coefficient = 0.toDouble
          }

          val factor = item_rating._2.filter(_._1 == user_item._1).map(r => r._2.toDouble)
          //println(factor.length)
          //        val weight_rating =item_rating._2.filter(r=>r._1 != user_item._2).map(r=>r._2.toDouble) //average on all other rated items
          //        val weight_avg = weight_rating.sum/weight_rating.length.toDouble

          val predict_factor = factor(0) * Coefficient

          //println(item_rating._1, predict_factor, Coefficient)

          (item_rating._1, predict_factor, Coefficient)

        }

        val relative_user_prediction = relative_item_record.map(formula_Coefficient).sortBy(-_._3).slice(0,3)//.take(5) //((rela_user3,coe3),...)

        //      val target_weight = target_record._2.filter(r=>r._1 != user_item._2).map(r=>r._2.toDouble)
        //      val target_weight_avg = target_weight.sum/target_weight.size.toDouble
        val k = relative_user_prediction.map(e => e._2)
        val p = relative_user_prediction.map(e => math.abs(e._3))
        var prediction = if (p.sum == 0) ave.sum/ave.length else k.sum / p.sum

        ((user_item._1, user_item._2), prediction)

      }

      val prediction = test.map(r => r._1).map(pearson)
      //.foreach(e=>println(e))//(user,item)
      //val check = test.map(r=>((r._1,r._2),r._3.toDouble))
      val rating_array = prediction.map(e => e._2).collect()


      val ratesAndPreds = prediction.join(test) //.map(r=>math.pow(r._2._1 - r._2._2,2)).mean()

      val category = ratesAndPreds.map(r => math.abs(r._2._1 - r._2._2)).collect()
      val zero_one = category.filter(_ >= 0).count(_ < 1)
      val one_two = category.filter(_ >= 1).count(_ < 2)
      val two_three = category.filter(_ >= 2).count(_ < 3)
      val three_four = category.filter(_ >= 3).count(_ < 4)
      val four = category.count(_ >= 4)
      val RMSE = math.sqrt(ratesAndPreds.map(r => math.pow(r._2._1 - r._2._2, 2)).mean())

      println(s">=0 and <1 $zero_one")
      println(s">=1 and <2 $one_two")
      println(s">=2 and <3 $two_three")
      println(s">=3 and <4 $three_four")
      println(s">=4 $four")
      println(s"RMSE $RMSE")


    //define outputfilename
    var outputname = args(2)
    val pw = new PrintWriter(new File(outputname))
    prediction.map(r=>(r._1._1.toInt,r._1._2.toInt,r._2)).collect().map(r=> if(r._1 < r._2) (r._1,r._2,r._3) else (r._2,r._1,r._3) )
      .sortBy{ case (x,y,z) => (x,y) }.map(e=>e._1 + "," + e._2 + "," + e._3.toString + "\n").foreach(pw.write)
    pw.close


    //timer
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.0
    println(s"Time(sec) $durationSeconds")

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

