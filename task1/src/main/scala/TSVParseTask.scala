import utils.{ETLUtils, PipelineTask}

import scala.io.Source

class TSVParseTask(val in: String, val out: String) extends PipelineTask {

  val tsvDelimiter = "\t"
  val newLineDelimiter = "\n"

  val specChar1 = '\t' //tab in malformed TSV? a little bit strange
  val specChar2 = '\r'
  val specChar3 = '\n'

  var correctList = Array[String]()

  override def taskName = "TSV parser"

  override def canRun: Boolean = ETLUtils.isSuccess(in)

  override def canSkip: Boolean = ETLUtils.isSuccess(out)

  override def run(): Unit = {
    def escape(field: String): String = {
      if (field.contains(specChar1) || field.contains(specChar3) || field.contains(specChar3)) s"'$field'" else field
    }

    Source.fromFile(in, "UTF-16LE").getLines().drop(1).foreach { line =>
      val tokens = line.split(tsvDelimiter)
      if (tokens.length == 5) {
        correctList :+= tokens.map(escape).mkString(tsvDelimiter)
      }
    }

    import java.io.PrintWriter
    new PrintWriter(out) {
      correctList foreach { l => write(s"$l$newLineDelimiter") }
      close
    }
  }

  override def complete(): Unit = {
    ETLUtils.complete(out)
  }
}