package net.jgp.books.sparkWithJava.ch09.lab900.genericPrettyPrinter;

public class GenericPrettyPrinterApp {

  public static void main(String[] args) {
    GenericPrettyPrinterApp app = new GenericPrettyPrinterApp();
    app.start();
  }

  private void start() {
    Book b = new Book();
    b.setTitle("Spark with Java");
    b.setAuthor("Jean Georges Perrin");
    b.setIsbn("9781617295522");
    b.setPublicationYear(2019);
    b.setUrl("https://www.manning.com/books/spark-with-java");
    
    Author a = new Author();
    a.setName("Jean Georges Perrin");
    a.setDob( "1971-10-05");
    a.setUrl("https://en.wikipedia.org/wiki/Jean_Georges_Perrin");
    
    System.out.println("A book...");
    PrettyPrinterUtils.print(b);
    
    System.out.println("An author...");
    PrettyPrinterUtils.print(a);
  }

}
