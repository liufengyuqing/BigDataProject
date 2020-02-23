package com.imooc.scala.course04

/**
 * @ClassName: ConstructorApp
 * @Description: TODO
 * @Create by: liuzhiwei
 * @Date: 2020/2/11 2:36 下午
 */

object ConstructorApp {
  def main(args: Array[String]): Unit = {
    //    val person = new Person("zhangsan", 30)
    //    println(person.name + ":" + person.age + ":" + person.school)
    //
    //
    //    val person2 = new Person("pk", 18, "m")
    //    println(person2.name + person2.age + person2.school + person2.gender)

    val student = new Student("pk", 18, "math")
    println(student.name + student.age + student.major)

    println(student.toString)


  }

}


//主构造器
class Person(val name: String, val age: Int) {
  println("Person Constructor enter...")

  val school = "ustc"
  var gender: String = _

  //附属构造器
  def this(name: String, age: Int, gender: String) {
    this(name, age) // 附属构造器的第一行代码必须调用主构造器或者其他附属构造器
    this.gender = gender
  }


  println("Person Construnctor leave")


}

class Student(name: String, age: Int, var major: String) extends Person(name, age) {
  println("Person Student Constructor enter...")


  override val school: String = "peking"

  override def toString: String = "Person :override def toString" + school

  println("Person Student Construnctor leave")
}