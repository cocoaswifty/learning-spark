/**
  * Created by jianhao on 2017/2/3.
  */

package sia

import org.apache.spark.sql.SparkSession

import scala.io.Source

object Day06App {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
    .appName("GitHub push counter")
    .master("local[*]")
    .getOrCreate()

    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "/Desktop/SMACK/learning-spark/Day6SecondApp/github-archive/2015-03-01-0.json"
    val githubLog = spark.read.json(inputPath)
    val pushes = githubLog.filter("type ='PushEvent'")  //filter欄位type為PushEvent的資料

    val grouped = pushes.groupBy("actor.login").count()
    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)

    val empPath = homeDir + "/Desktop/SMACK/learning-spark/Day6SecondApp/ghEmployees.txt"
    val employees = Set() ++ (    //用++將yield line.trim產生的集合物件放入空的Set中
      for {
        line <- Source.fromFile(empPath).getLines()
      } yield line.trim
    )

    //輸入一個字串，若有出現在employees中則回傳True，否則就回傳False
    val isEmp: String => Boolean = {    //String => Boolean: 代表的是這個變數的型態
      name => employees.contains(name)
    }

    val isEmployee = spark.udf.register("isEmpUdf", isEmp)  //SparkSQL User Define Fuction(UDF)註冊成UDF

    import spark.implicits._    //匯入implicits函數
    val filtered = ordered.filter(isEmployee($"login"))     //接著就能透過UDF比對order中的資料了，ordered RDD透過filter方式呼叫isEmployee進行過慮
    filtered.show(5)

  }

}










