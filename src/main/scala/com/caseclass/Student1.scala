package caseclass

case class Student1 (name:String, age:Int)

object Main
{
  // Main method
  def main(args: Array[String])
  {
    val s1 = Student1("Nidhi", 23)

    // Display parameter
    println("Name is " + s1.name);
    println("Age is " + s1.age);
    val s2 = s1.copy(age = 24)

    // Display copied and changed attributes
    println("Copy Name is " + s2.name);
    println("Change Age is " + s2.age);
  }
}
