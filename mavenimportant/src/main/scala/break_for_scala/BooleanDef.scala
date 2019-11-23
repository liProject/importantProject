package break_for_scala

object BooleanDef {
  def main(args: Array[String]): Unit = {
    // for循环

    var flag = true
    var res = 0

    for (i <- 0 until 10 if flag) {
      res += i
      println("res = " + res)
      if (i == 5) flag = false
    }
  }
}
