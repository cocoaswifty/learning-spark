//http://ithelp.ithome.com.tw/users/20103839/ironman/1210

/*
Scala中val為immutable;var為mutable
*/

//MARK:Day2 - RDD概念與map操作
val lines = sc.textFile("./hello.txt")	//初始化一個SparkContext物件sc，使用textFile函式逐行讀取檔案，並轉成名為lineRDD
val lineCounts = lines.count 		//使用count讀取集合元素數量

val bsdLines = lines.filter(line=>line.contains("BSD"))	//取得含有某些特定字串的那幾行
bsdLines.foreach(bLine=>println(bLine))					//把結果印出到console
bsdLines.foreach(println)								//省略寫法

val numbers=sc.parallelize(List(1,2,3,4,5))	//parallelize函式將List集合物件轉換成RDD物件
val numberSquared=numbers.map(num=>num*num)	//每個numbers的元素都會被平方
numberSquared.foreach(num=>print(num+" "))	//印出結果，foreach這個函式操作是平行印出會導致順序變動
numberSquared.first							//first取出集合中的第一個元素
numberSquared.top(2)						//top(N)對整數來說就是取出最大的N個
numberSquared.collect()						//collect()的action操作會返還一個新的非RDD的普通集合物件

val castToReverseString=numberSquared.map(_.toString.reverse).collect	//toString轉成字串，再接reverse函式


//MARK:Day3 - RDD概念與flatMap操作
---client-ids.log---
15,16,17,20,20
77,80,94
30,52,13,36
20,31,15

val lines = sc.textFile("./client-ids.log")
val idsStr=lines.map(line=>line.split(","))	//split(",")函式把逗點分隔的資料切開
idsStr.collect
val ids= lines.flatMap(_.split(","))		//flatMap將集合內的集合攤平
ids.collect
ids.distinct.count 							//distinct函式消去重複元數


//MARK:Day4 - Scala & RDD中的Implicit Conversion
class ClassOne[T](val input: T) { }	//宣告一個 type parameterized的Class類別

//宣告了ClassOneStr與ClassOneInt兩個一般類別，並各自擁有ClassOne[String]與ClassOne[Int]名稱為one的類別成員。並且各自宣告了一個duplicatedXXX的簡易函式，一個重複Str，一個重複Int
class ClassOneStr(val one: ClassOne[String]) {
       def duplicatedString() = one.input + one.input
       }
class ClassOneInt(val one: ClassOne[Int]) {
       def duplicatedInt() = one.input.toString + one.input.toString
       }

//宣告兩個隱式轉換函式toStrMethods與toIntMethods
implicit def toStrMethods(one: ClassOne[String]) = new ClassOneStr(one)
implicit def toIntMethods(one: ClassOne[Int]) = new ClassOneInt(one)

val oneStrTest= new ClassOne("test")	//oneStrTest使用時會幫你轉成ClassOneStr，這樣就能用ClassOneStr的duplicatedString()啦，並且保證型別是對的
oneStrTest.duplicatedString

val oneIntTest = new ClassOne(123)		//若宣告為Int類別，呼叫duplicatedString就會報錯
oneIntTest.duplicatedString


//MARK:Day6 - For expression、 Set 、 SparkSQL UDF by Use Case
for (i <- 1 to 5) yield i * 2	//遍歷1到5，然後將每個元素都*2，並放入另外一個集合物件中
for (i <- 1 to 5 by 2) yield i * 2	//不想遍歷1到5，想一次跳2(也就是1,3,5 )


//MARK:Day8 - Pair RDD-1
val pair = sc.parallelize(List((1,3),(3,5),(3,9)))	//建立pairRDD最簡單的方式就是傳一個集合給sc.parallelize，其中每個元素都包含兩個值(也就是Tuple)
pair.keys.collect		//分別抓出keys與values
pair.values.collect

pair.groupByKey.collect	//用key值分群
pair.mapValues(x=>x+1).collect	//對每個KV中的V執行某個func，但K值不變
pair.mapValues(_+1).collect		//簡寫
pair.flatMapValues(x=>(x to 6)).collect	//flatMap 集合會被攤平，每個元素會搭配原本的K
/** (1,3)變成[(1,3),(1,4),(1,5),(1,6)]
  * (3,5)變成[(3,5),(3,6)]
  * (3,9)為空(9超過6了)
  * 然後再將(1,3),(1,4),(1,5),(1,6),(3,5),(3,6)攤平收集起來成為一個結果
  */

pair.reduceByKey((x,y)=>x+y).collect	//V要ByKey組合，並傳入一個函式說明同key的V如何被處理
pair.reduceByKey(_+_).collect			//簡寫

pair.countByKey	//將同樣key的資料分群，然後對每個K收集到的元素count，結果表示K=1的元素有1個，K=3的元素有2個
pair.sortByKey().collect	//按key排序

//Task.1 搜尋消費最多的使用者，額外贈予一支bear doll(ID=4)
val transFile= sc.textFile("data_products.txt")					//sc讀檔
val transData = transFile.map(_.split("#"))						//將每行字串切開成為矩陣
var transByCust = transData.map(tran => (tran(2).toInt,tran))	//從每個矩陣中的抽出顧客ID(也就是陣列中的第3個元素tran(2)）與陣列本身，並透過map成新的RDD。最終會成為一個[Int, Array]的pairRDD
transByCust.keys.distinct.count 	//總共有幾個不同的客戶
transByCust.countByKey		//將同樣key的資料放在一起，然後count
transByCust.countByKey.toSeq	//toSeq轉換集合型態
transByCust.countByKey.toSeq.sortBy(_._2)	//sortBy(pair=>pair._2)，pair._2代表取KV中的V，若要取KV元素的K則寫成(KVvariable._1)的格式
transByCust.countByKey.toSeq.sortBy(pair=>pair._2).last	//因為排序是ascending，所以我們取最後一筆
val (customerID, purchNum) = transByCust.countByKey.toSeq.sortBy(pair=>pair._2).last	//宣告兩個變數來分別接KV
var complTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"))	//資料暫存到一個一般集合物件，customerID=53、bear doll ID為4、商品數量為1、金額為0

//Task.2 一次購買Barbie兩隻以上給予結帳金額95%優惠
transByCust = transByCust.mapValues(tran => {
    if(tran(3).toInt == 25 && tran(4).toInt > 1)		//若商品ID與數量(>1)吻合
        tran(5) = (tran(5).toDouble * 0.95).toString 	//修改結帳金額(打95折)
    tran 	//回傳交易紀錄
})

//Task.3 購買五本字典的客戶贈予一支牙刷
transByCust = transByCust.flatMapValues(tran => {
	if(tran(3).toInt == 81 && tran(4).toInt >= 5) {
		val cloned = tran.clone()	//若符合條件則clone一筆紀錄,不clone自己建一個String Array也OK
		cloned(5) = "0.00"; cloned(3) = "70"; cloned(4) = "1";	//更新陣列值
		List(tran, cloned)	//回傳一個元素的集合物件，之後會攤平，所以要用一個List將兩個Array wrapper起來
	} 
	else 
		List(tran)	//不符合條件的話只能wrapper原始的tran陣列
})

//Task.4 贈予總消費金額最高的客戶一件睡衣
val amouts = transByCust.mapValues(t=>t(5).toDouble)	//將紀錄轉換成簡單的(customerID,消費紀錄)
amouts.collect
val total = amouts.reduceByKey(_+_)	//消費金額加總
total.collect
val total = amouts.reduceByKey(_+_).collect.sortBy(_._2)	//排序
complTrans = complTrans :+ Array("2015-03-30", "11:59 PM", "76", "63", "1", "0.00")	//新增一筆紀錄到complTrans集合物件中，:+ 將元素append在Array最後
transByCust = transByCust.union(sc.parallelize(complTrans).map(t => (t(2).toInt, t)))	//最後將complTrans寫入transByCust中
/** sc.parallelize(complTrans)將complTrans轉成RDD
  * .map(t =>(t(2).toInt, t))將complTrans轉成符合transByCust的格式:PairRDD
  * 透過union將合併兩個RDD
  */


//MARK:Day10 - Currying
def addCurrying(x: Int) = (y: Int) => x + y 	//傳入一個Int，而輸出為Int的函式
addCurrying(5)(7)	//addCurrying(5)會回傳y=5+y的匿名函式，然後(7)再帶入該函式得到12

def addScalaCurrying(x: Int)(y: Int) = x + y 	//這種寫法是Scala對Currying提供的語法蜜糖
addScalaCurrying(5)(7)

def concatenator(w1: String): String => String = { w2 => w1 + " " + w2 }	//定義一個函數concatenator，其輸出也是一個函數
val gretting = concatenator("Hi")	//呼叫一個concatenator("Hi")，仔細看會回傳(w2 => "Hi" + " " +w2)的一個匿名函式給greeting
gretting("Joe")		//對greeting傳入不同參數的效果
gretting("Readers")	

def concatenatorCurried(w1: String)(w2: String) = w1 + " " + w2		//定義一個Currying的concatenator，參數寫法改為兩個(para1)(para2)，輸出是一般型別而不是函式了
concatenatorCurried("Hi")("Joe")
val greetingPrefix = concatenatorCurried("Hi")_ 	//若我確定greeting都會是Hi，只會換名子，那我可以宣告成concatenatorCurried("Hi")_，_就類似placeholder的概念
greetingPrefix("Joe")										//對greeting傳入參數的效果
val GrettingToJoe = concatenatorCurried(_:String)("Joe")	//Currying化：調整一下：concatenatorCurried(_:String)("Joe")即可
GrettingToJoe("How are you")
GrettingToJoe("Hi")




def isInRange(from: Int, to: Int)(point: Int) = {	//currying的一般化函式，接收兩個參數(from: Int, to: Int)(point: Int)
		if (point >= from && point <= to) true
		else false
	}

val fromZeroToTen = isInRange(0, 10) _
fromZeroToTen(5)
fromZeroToTen(10)
val isFiveInRange = isInRange(_: Int, _: Int)(5)
isFiveInRange(0, 10)
isFiveInRange(50, 100)





