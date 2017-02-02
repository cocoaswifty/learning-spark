//http://ithelp.ithome.com.tw/users/20103839/ironman/1210

/*
Scala中val為immutable;var為mutable
*/

//MARK:RDD概念與map操作
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


//MARK:RDD概念與flatMap操作
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

