/**
 * create by liuzhiwei on 2020/3/19
 *
 */
object First {
  def main(args: Array[String]): Unit = {
    val i = 1
    val arr = Array[Int](1, 2, 3)
    for (e <- arr) {
      print(e + " ")
      // println(e)
    }
    println()

    for (i <- 1 to 3; j <- 1 to 3 if i != j) {
      println((i + " " + j) + " ")
    }


    println("\nhello world")
  }

}
