package net.jgp.books.spark.ch09.lab900_generic_pretty_printer

import java.lang.reflect.{InvocationTargetException, Method}

object PrettyPrinterScalaUtils {

  /**
   * Pretty prints the content of a Javabean by looking at all getters in an
   * object and by calling them.
   *
   * @param o
   * The instance of an object to introspect.
   */
  def print(o: Any): Unit = {
    val methods = o.getClass.getDeclaredMethods
    for (i <- 0 until methods.length) {
      val method = methods(i)
      if (!isGetter(method)) 
        sys.exit(1)
      
      val methodName = method.getName
      print(methodName.substring(3))
      print(": ")
      try // Invoke the method on the object o
        println(method.invoke(o))
      catch {
        case e: IllegalAccessException =>
          println(s"The method $methodName raised an illegal access exception as it was called: ${e.getMessage}")
        case e: IllegalArgumentException =>
          println(s"The method $methodName raised an illegal argument exception as it was called (it should not have any argument): ${e.getMessage}")
        case e: InvocationTargetException =>
          println(s"The method $methodName raised an invocation taregt exception as it was called: ${e.getMessage}")
      }
    }
  }
  
  /**
   * Return true if the method passed as an argument is a getter, respecting
   * the following definition:
   * <ul>
   * <li>starts with get</li>
   * <li>does not have any parameter</li>
   * <li>does not return null
   * <li>
   * </ul>
   *
   * @param method
   * method to check
   * @return
   */
  private def isGetter(method: Method): Boolean = {
    if (!method.getName.startsWith("get")) 
      return false
    if (method.getParameterTypes.length != 0) 
      return false
    if (classOf[Unit] == method.getReturnType) 
      return false
    true
  }
  
}
