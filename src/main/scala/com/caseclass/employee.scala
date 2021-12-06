package com.caseclass

case class employee(name:String, age:Int)
object Main
{
  // Main method
  def main(args: Array[String])
  {
    var res = employee("Roshan", 23)

    // Display both Parameter
    println("Name of the employee is: " + res.name);
    println("Age of the employee is: " + res.age);
  }
}
