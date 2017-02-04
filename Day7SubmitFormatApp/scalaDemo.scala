/**
  * Created by jianhao on 2017/2/3.
  */

package SparkIronMan

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.io.Source

object Day07_SubmitFormat {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext

    val githubLog = spark.read.json(args(0))  //檔案路徑改為吃參數
    val pushes = githubLog.filter("type ='PushEvent'")  //filter欄位type為PushEvent的資料
    val grouped = pushes.groupBy("actor.login").count()
    val ordered = grouped.orderBy(grouped("count").desc)

    val employees = Set() ++ (    //用++將yield line.trim產生的集合物件放入空的Set中
      for {
        line <- Source.fromFile(args(1)).getLines()
      } yield line.trim
    )

    val bcEmployees = sc.broadcast(employees)             //透過sc.broadcast將變數註冊成廣播變數
    import spark.implicits._    //匯入implicits函數
    val isEmp: String => Boolean = (name => bcEmployees.value.contains(name)) //要取出廣播變數值要透過bcEmployees.value
    val isEmployee = spark.udf.register("isEmpUdf", isEmp)  //SparkSQL User Define Fuction(UDF)註冊成UDF
    val filtered = ordered.filter(isEmployee($"login"))     //接著就能透過UDF比對order中的資料了，ordered RDD透過filter方式呼叫isEmployee進行過慮
    filtered.write.format(args(3)).save(args(2))  //write支援多種輸出

  }

}















