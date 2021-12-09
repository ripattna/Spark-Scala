/*
The most benefit of Case Class is that Scala Compiler affix a method with the name of the class having identical number of parameters
as defined in the class definition, because of that you can create objects of the Case Class even in the absence of the keyword new.
 */

package caseclass

case class Book (name:String, author:String)
object Main
{
  // Main method
  def main(args: Array[String])
  {
    var Book1 = Book("Data Structure and Algorithm", "cormen")
    var Book2 = Book("Computer Networking", "Tanenbaum")

    // Display strings
    println("Name of the Book1 is " + Book1.name);
    println("Author of the Book1 is " + Book1.author);
    println("Name of the Book2 is " + Book2.name);
    println("Author of the Book2 is " + Book2.author);
  }
}
