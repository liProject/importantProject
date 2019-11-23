package break_for_scala

object ReturnDef {
  /**
    * 1 + 2 + 3 + 4
    *
    * @return
    */
  def addOuter() = {
    var res = 0
    def addInner() {
      for (i <- 0 until 10) {
        if (i == 5) {
          return
        }
        res += i
        println("res = " + res)
      }
    }
    addInner()
    res
  }

  def main(args: Array[String]): Unit = {
    addOuter()
  }
}
