package net.jgp.books.spark.ch09.lab900_generic_pretty_printer

/**
 * Simple pretty printer application showing the usage of the pretty printer
 * using reflection.
 *
 * @author rambabu.posa
 *
 */
object SimplePrettyPrinterScalaApp{

  /**
   * Start the application
   */
  def main(args: Array[String]): Unit = {

    // Create a book
    val b = new Book
    b.setTitle("Spark with Java")
    b.setAuthor("Jean Georges Perrin")
    b.setIsbn("9781617295522")
    b.setPublicationYear(2019)
    b.setUrl("https://www.manning.com/books/spark-with-java")

    // Create an author
    val a = new Author
    a.setName("Jean Georges Perrin")
    a.setDob("1971-10-05")
    a.setUrl("https://en.wikipedia.org/wiki/Jean_Georges_Perrin")

    // Dumps the result
    println("A book...")
    PrettyPrinterUtils.print(b)
    println("An author...")
    PrettyPrinterUtils.print(a)

  }

}
