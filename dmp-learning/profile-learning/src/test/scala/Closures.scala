object Closures {

  /**
   * @Author li
   * @Description scala的闭包
   * @Date 20:22 2019/11/26
   * @Param [args]
   * @return void
   **/
  def main(args: Array[String]): Unit = {
    val addOne: Int => Int = makeAdd(1)
    val addTwo: Int => Int = makeAdd(2)
    
    println(addOne(2))
    println(addTwo(1))
  }
  
  def makeAdd(more: Int) = (x: Int) => {
    println(x+"+++")
    x + more
  }
  
//  def normalAdd(a: Int, b: Int) = a + b
  
}