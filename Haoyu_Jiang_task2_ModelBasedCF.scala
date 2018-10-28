import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.{SparkConf, SparkContext}
import java.io._
import scala.io.Source

object ModelBasedCF{


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

    val whole = wholefile.filter(row => row != headerwhole).map(r=>r.split(",")).map(r=>(r(0),r(1),r(2)))
    val wholemap = whole.map(r=>(r._1,(r._2,r._3))).groupByKey().collect().toMap//user,item,rating

    val test = testfile.filter(row => row != headertest).map(r=>r.split(",")).map(r=>(r(0),r(1),r(2)))//.collect()//user,item,rating
    val test_filter = test.collect()
    val test_predict = test.map(_ match { case (user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)})
    //val train = whole.filter(r=> ! test.contains(r))//.map(r=> if (test.contains(r)) (r(0),r(1),None) else (r(0),r(1),r(2)) )

    val ratings = whole.filter(!test_filter.contains(_)).map(_ match { case (user, item, rate) => Rating(user.toInt, item.toInt, rate.toDouble)})

    // Build the recommendation model using ALS
    val rank = 3
    val numIterations = 10
    val model = ALS.train(ratings, rank, numIterations, 0.25,1,1)

    // Evaluate the model on rating data
    val usersProducts = test_predict.map { case Rating(user, product, rate) =>
      (user, product)
    }

    val out= model.predict(usersProducts)
    val predictions =
     out.map { case Rating(user, product, rate) =>
        ((user, product), rate)
      }
    val rating_array = predictions.map(e=>e._2).collect()
    val min_rate = rating_array.min
    val max_rate = rating_array.max
    val difference = max_rate-min_rate

    val nomlized_predictions = predictions.map(r=>(r._1,((r._2-min_rate)/difference.toDouble)*5))


    val ratesAndPreds = test_predict.map { case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(nomlized_predictions)



    val category = ratesAndPreds.map(r=>math.abs(r._2._1-r._2._2)).collect()
    val zero_one = category.filter(_>=0).count(_<1)
    val one_two = category.filter(_>=1).count(_<2)
    val two_three = category.filter(_>=2).count(_<3)
    val three_four = category.filter(_>=3).count(_<4)
    val four = category.count(_>=4)
    val RMSE = math.sqrt(ratesAndPreds.map { case ((user, product), (r1, r2)) =>
      val err = r1 - r2
      err * err
    }.mean())

    println(s">=0 and <1 $zero_one")
    println(s">=1 and <2 $one_two")
    println(s">=2 and <3 $two_three")
    println(s">=3 and <4 $three_four")
    println(s">=4 $four")
    println(s"RMSE $RMSE")


    //define outputfilename
    var outputname = args(2)
    val pw = new PrintWriter(new File(outputname))
    nomlized_predictions.map(r=>(r._1._1,r._1._2,r._2)).collect().map(r=> if(r._1 < r._2) (r._1,r._2,r._3) else (r._2,r._1,r._3) )
      .sortBy{ case (x,y,z) => (x,y) }.map(e=>e._1 + "," + e._2 + "," + e._3.toString + "\n").foreach(pw.write)
    pw.close


    //timer
    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000.0
    println(s"Time(sec) $durationSeconds")

  }

}


