import org.apache.spark.{SparkConf, SparkContext}
import java.io._
import scala.io.Source


object UserBasedCF {
  //var whole : RDD[(String,String,String)]

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

    val whole = wholefile.filter(row => row != headerwhole).map(r=>r.split(",")).map(r=>(r(0),r(1),r(2).toDouble)).map(r=>((r._1,r._2),r._3))//.collect()
    val test = testfile.filter(row => row != headertest).map(r=>r.split(",")).map(r=>(r(0),r(1),r(2).toDouble)).map(r=>((r._1,r._2),r._3))//.collect()//user,item,rating
    val train = whole.subtractByKey(test).map(r=>(r._1._1,(r._1._2,r._2))).groupByKey().collect()//.map(r=>(r._1,r._2.map(e=>e._2)))//user,((item,rating)...)


    def pearson(user_item:(String,String)) : ((String,String),Double) ={

      var relative_user = Array[(String,Array[(String,Double)])]() //.map(e=>(e._1,e._2.toArray))

      for (record<-train){
        if (record._1 != user_item._1 && record._2.map(r=>r._1).toArray.contains(user_item._2)){
          relative_user =  (record._1,record._2.toArray) +: relative_user
        }
      }

      val target_record =train.filter(_._1 == user_item._1)(0)
      //println(target_record.length)

      def formula_Coefficient(item_rating:(String,Array[(String,Double)])):(String,Double,Double)={

        val target_itemset = target_record._2.toArray.map(r=>r._1)
        val relative_user_itemset = item_rating._2.map(e=>e._1)
        val shared_item = target_itemset.intersect(relative_user_itemset)

        if (shared_item.length == 0) {return  (item_rating._1, 0.0, 0.0)}

        val target_rating = target_record._2.filter(r => shared_item.contains(r._1)).toArray.sortBy(_._1).map(r=>r._2.toDouble)
        val target_rating_avg = target_rating.sum/target_rating.length.toDouble

        val relative_user_rating = item_rating._2.filter(r=>shared_item.contains(r._1)).sortBy(_._1).map(r=>r._2.toDouble)
        val relative_user_rating_avg = relative_user_rating.sum/relative_user_rating.length.toDouble


        val Denominator_target = math.pow(target_rating.map(r=>math.pow(r-target_rating_avg,2)).sum,0.5)
        val Denominator_relative = math.pow(relative_user_rating.map(r=>math.pow(r-relative_user_rating_avg,2)).sum,0.5)
        var Coefficient  = target_rating.map(r=>r-target_rating_avg).zip(relative_user_rating.map(r=>r-relative_user_rating_avg))
          .map(e=>e._1*e._2).sum / Denominator_relative * Denominator_target //average on co_reated items

        if (Coefficient.isNaN){
          Coefficient = 0.toDouble
        }

        val factor = item_rating._2.filter(_._1 == user_item._2).map(r=>r._2.toDouble)
        //println(factor.length)
        val weight_rating =item_rating._2.filter(r=>r._1 != user_item._2).map(r=>r._2.toDouble) //average on all other rated items
        val weight_avg = weight_rating.sum/weight_rating.length.toDouble

        val predict_factor = (factor(0)-weight_avg)*Coefficient

        //println(item_rating._1, predict_factor, Coefficient)

        (item_rating._1, predict_factor, Coefficient)

      }

      val relative_user_prediction =relative_user.map(formula_Coefficient).sortBy(- _._3)//.slice(0,20)//.take(5) //((rela_user3,coe3),...)

      val target_weight = target_record._2.filter(r=>r._1 != user_item._2).map(r=>r._2.toDouble)
      val target_weight_avg = target_weight.sum/target_weight.size.toDouble
      val k = relative_user_prediction.map(e=>e._2)
      val p = relative_user_prediction.map(e=>math.abs(e._3))
      var prediction = target_weight_avg + (if (p.sum == 0) 0 else k.sum /p.sum)
//      if (prediction.isNaN){
//        prediction = 0.toDouble
//      }
      ((user_item._1,user_item._2),prediction)

    }

    val prediction = test.map(r=>r._1).map(pearson)//.foreach(e=>println(e))//(user,item)
    //val check = test.map(r=>((r._1,r._2),r._3.toDouble))
    val rating_array = prediction.map(e=>e._2).collect()


    val ratesAndPreds = prediction.join(test)//.map(r=>math.pow(r._2._1 - r._2._2,2)).mean()

    val category = ratesAndPreds.map(r=>math.abs(r._2._1-r._2._2)).collect()
    val zero_one = category.filter(_>=0).count(_<1)
    val one_two = category.filter(_>=1).count(_<2)
    val two_three = category.filter(_>=2).count(_<3)
    val three_four = category.filter(_>=3).count(_<4)
    val four = category.count(_>=4)
    val RMSE = math.sqrt(ratesAndPreds.map(r=>math.pow(r._2._1 - r._2._2,2)).mean())

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

}


