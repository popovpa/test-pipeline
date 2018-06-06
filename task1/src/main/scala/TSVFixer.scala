import scala.io.Source

object TSVFixer extends App {

  val tsvDelimiter = "\t"
  val newLineDelimiter = "\n"

  val specChar1 = '\t' //tab in malformed TSV? a little bit strange
  val specChar2 = '\r'
  val specChar3 = '\n'

  var correctList = Array[String]()

  Source.fromFile("data.tsv", "UTF-16LE").getLines().drop(1).foreach { line =>
    var tokens = line.split(tsvDelimiter)
    if (tokens.length == 5) {
      correctList :+= tokens.map(escape).mkString(tsvDelimiter)
    }
  }

  import java.io.PrintWriter
  new PrintWriter("data_correct.tsv") {
    correctList foreach { l => write(s"$l$newLineDelimiter") };
    close;
  }

  def escape(field: String): String = {
    if (field.contains(specChar1) || field.contains(specChar3) || field.contains(specChar3)) s"'$field'" else field
  }
}