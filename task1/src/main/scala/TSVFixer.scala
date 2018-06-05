import scala.StringContext
import scala.io.Source

object TSVFixer extends App {

  var correctList:Array[String] = Array[String]()

  Source.fromFile("data.tsv", "UTF-16LE").getLines().drop(1).foreach { line =>
    val tokens = line.split('\t')
    if (tokens.length == 5) {
      correctList :+= tokens.mkString(",")
    }
  }
  println(correctList)

  def escape(string:String):String={



  }
}

case class Item(id: String, first_name: String, last_name: String, account_number: Long, email: String)