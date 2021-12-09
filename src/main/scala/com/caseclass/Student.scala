package caseclass

case class Student (name:String, age:Int)
object Main
{
  // Main method
  def main(args: Array[String])
  {
    val s1 = Student("Nidhi", 23)

    // Display parameter
    println("Name is " + s1.name);
    println("Age is " + s1.age);
    val s2 = s1.copy()

    // Display copied data
    println("Copy Name " + s2.name);
    println("Copy Age " + s2.age);
  }
}
