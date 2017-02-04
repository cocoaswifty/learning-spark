/**
  * Created by jianhao on 2017/2/3.
  */

import org.apache.spark.sql.SparkSession

object FirstApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
    .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "/Desktop/SMACK/learning-spark/Day5FirstApp/github-archive/2015-03-01-0.json"
    val githubLog = spark.read.json(inputPath)
    val pushes = githubLog.filter("type ='PushEvent'")  //filter欄位type為PushEvent的資料

    githubLog.printSchema   //印出githubLog DataSet的綱要資訊
    println("all events:" + githubLog.count)  //印出githubLog與pushes的筆數
    println("push events:" + pushes.count)
    pushes.show(5)    //show類似DB的select語法，在此印出5筆

    val grouped = pushes.groupBy("actor.login").count()
    grouped.show(5)

    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)
  }

}
















