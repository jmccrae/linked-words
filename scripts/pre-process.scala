import scala.util.Random
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream
import java.io.FileInputStream

object PreProcess {
  private val pattern1 = Pattern.compile("(\\.\\.\\.+|[\\p{Po}\\p{Ps}\\p{Pe}\\p{Pi}\\p{Pf}\u2013\u2014\u2015&&[^'\\.]]|(?<!(\\.|\\.\\p{L}))\\.(?=[\\p{Z}\\p{Pf}\\p{Pe}]|\\Z)|(?<!\\p{L})'(?!\\p{L}))")
  private val pattern2 = Pattern.compile("\\p{C}|^\\p{Z}+|\\p{Z}+$")

  // My "pretty good" tokenizer... works well on most languages
  def tokenize(s : String) = {
    val s1 = pattern1.matcher(s).replaceAll(" $1 ")
    val s2 = pattern2.matcher(s1).replaceAll("");
    s2.split("\\p{Z}+")
  }

  def isWord(w : String) = {
    w.matches(".*\\p{L}.*")
  }


  val docLine = "<doc .* title=\"(.*)\">".r
  val endDocLine = "</doc>".r

  def printWriter(fileName : String) = {
    if(fileName.endsWith(".gz")) {
      new java.io.PrintWriter(
        new java.util.zip.GZIPOutputStream(
          new java.io.FileOutputStream(fileName)))
    } else {
      new java.io.PrintWriter(fileName)
    }
  }

  def main(args : Array[String]) {
    var lines = 0
    var goodDoc = false
    var docId = Random.nextLong()
    val fileId = args(0)
    val docsOut = printWriter(fileId + ".docs.gz")
    val linesOut = printWriter(fileId + ".raw.gz")
    val tokenizedOut = printWriter(fileId + ".unsorted.gz")
    val input = io.Source.fromInputStream(
        new GZIPInputStream(new FileInputStream(fileId + ".xml.gz")))
    var tokensTotal = 0
    var words = collection.mutable.Set[String]()
    for(line <- input.getLines) {
	  lines += 1
          if(lines % 100000 == 0) {
             System.err.print(".")
             System.err.flush()
          }
	  line match {
            case docLine(title) => {
	      // Title with colons in them tend to be meta and are often not in 
              // Wikipedia language
	      goodDoc = !title.contains(":")
              if(goodDoc) {
                docId = Random.nextLong()
                docsOut.println("%016x %s" format (docId, line))
              }
	    }
	    case endDocLine() => {
              goodDoc = false
	    }
            case line if goodDoc && !line.matches("\\s*") => {
              val idHex = ("%016x" format docId)
              val lineId = ("%08x" format Random.nextInt())
              val tokens = tokenize(line.toLowerCase)
                .filter(isWord)
              tokensTotal += tokens.size
              words ++= tokens
              if(!tokens.isEmpty) {
                linesOut.println(line + lineId + idHex)
                tokenizedOut.println(tokens.mkString(" ") + lineId + idHex)
              }
	    }
	    case _ => { // ignore 
	    }
	}
      }
      docsOut.close()
      linesOut.close()
      tokenizedOut.close()
      val statsOut = printWriter(fileId + ".stats.json")
      statsOut.println(s"""{
  "lang": "${fileId.dropRight(4)}",
  "collection": "$fileId",
  "tokens": ${tokensTotal},
  "types": ${words.size}
}""")
      statsOut.close()
     System.err.println()
   }
}
