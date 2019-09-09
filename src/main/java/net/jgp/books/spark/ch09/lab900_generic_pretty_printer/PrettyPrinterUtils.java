package net.jgp.books.spark.ch09.lab900_generic_pretty_printer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class PrettyPrinterUtils {

  /**
   * Pretty prints the content of a Javabean by looking at all getters in an
   * object and by calling them.
   * 
   * @param o
   *          The instance of an object to introspect.
   */
  public static void print(Object o) {
    Method[] methods = o.getClass().getDeclaredMethods();
    for (int i = 0; i < methods.length; i++) {
      Method method = methods[i];
      if (!isGetter(method)) {
        continue;
      }

      String methodName = method.getName();
      System.out.print(methodName.substring(3));
      System.out.print(": ");
      try {
        // Invoke the method on the object o
        System.out.println(method.invoke(o));
      } catch (IllegalAccessException e) {
        System.err.println("The method " + methodName
            + " raised an illegal access exception as it was called: "
            + e.getMessage());
      } catch (IllegalArgumentException e) {
        System.err.println("The method " + methodName
            + " raised an illegal argument exception as it was called (it should not have any argument): "
            + e.getMessage());
      } catch (InvocationTargetException e) {
        System.err.println("The method " + methodName
            + " raised an invocation taregt exception as it was called: "
            + e.getMessage());
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
   *          method to check
   * @return
   */
  private static boolean isGetter(Method method) {
    if (!method.getName().startsWith("get")) {
      return false;
    }
    if (method.getParameterTypes().length != 0) {
      return false;
    }
    if (void.class.equals(method.getReturnType())) {
      return false;
    }
    return true;
  }

}
