package caseclass

case class employee (name:String, age:Int)
object Main
{
  // Main method
  def main(args: Array[String])
  {
    var employee1 = employee("Nidhi", 23)

    // Display both Parameter
    println("Name of the employee is " + employee1.name);
    println("Age of the employee is " + employee1.age);

    //var employee1.age = 10
    //println("Age of the employee is " + employee1.age);
  }
}
