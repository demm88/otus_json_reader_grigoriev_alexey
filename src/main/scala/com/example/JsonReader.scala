package com.example

import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object JsonReader extends App {
  if (args.length == 0) {
    println("enter file name with path")
    sys.exit(1)
  }
  val filename = args(0)

  val conf = new SparkConf().setAppName("json_reader")
  val sc = new SparkContext(conf)

  case class ParsedJsonObj(id: Int = 0, country: String = "", points: Int = 0, price: Float = 0, title: String = "", variety: String = "", winery: String = "")

  val lines = sc.textFile(filename)
  lines.foreach(x => decodeJsonAndPrint(x))

  def decodeJsonAndPrint(x: String): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val json = parse(x)
    val obj = json.extract[ParsedJsonObj]
    println(obj)
  }
}
