/**
  * Created by jianhao on 2017/2/3.
  */

package SparkIronMan
import org.apache.spark.sql.SparkSession
import scala.io.Source

object Day07_Brodcast {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
    .appName("GitHub push counter")
    .master("local[*]")
    .getOrCreate()

    val homeDir = System.getenv("HOME")
    val inputPath = homeDir + "/Desktop/SMACK/learning-spark/Day7BrodcastApp/github-archive/2015-03-01-0.json"
    val githubLog = spark.read.json(inputPath)
    val pushes = githubLog.filter("type ='PushEvent'")  //filter欄位type為PushEvent的資料
    import spark.implicits._    //匯入implicits函數
    val grouped = pushes.groupBy("actor.login").count()
    val ordered = grouped.orderBy(grouped("count").desc)

    val empPath = homeDir + "/Desktop/SMACK/learning-spark/Day7BrodcastApp/ghEmployees.txt"
    val employees = Set() ++ (    //用++將yield line.trim產生的集合物件放入空的Set中
      for {
        line <- Source.fromFile(empPath).getLines()
      } yield line.trim
    )


    val sc = spark.sparkContext
    val bcEmployees = sc.broadcast(employees)             //透過sc.broadcast將變數註冊成廣播變數
    val isEmp: String => Boolean = (name => bcEmployees.value.contains(name)) //要取出廣播變數值要透過bcEmployees.value
    val isEmployee = spark.udf.register("isEmpUdf", isEmp)  //SparkSQL User Define Fuction(UDF)註冊成UDF
    val filtered = ordered.filter(isEmployee($"login"))     //接著就能透過UDF比對order中的資料了，ordered RDD透過filter方式呼叫isEmployee進行過慮
    filtered.show(5)

  }

}










