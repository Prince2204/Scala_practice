// Databricks notebook source
// DBTITLE 1,Variables in Scala
//Defining an immutable variable
val name = "Prince"
//Try to reassign a new value to above created variable 'name'. It will give error: reassignment to val
//name = "Raj"
//Defining a mutable variable
var city = "Dhanbad"
//Try to reassign a new value to above created variable 'city'. It will update the value
city = "Kolkata"

// COMMAND ----------

// DBTITLE 1,Collections : List
//Scala explicitly distinguishes between immutable versus mutable collections — right from the package namespace itself ( scala.collection.immutable or scala.collection.mutable).
//Ways to create a list:
// 1. By specifying List keyword
val names = List("Prince","Raj","Amar")
// 2. Using the cons :: operator. This joins a head element with the remaining tail of a list.
val name = "Prince" :: "Amar" :: "Raj" :: "Ram" :: Nil
//
// Accessing a List 
// by index value, scala follows zero based index
name(2)  // Will retun the element at index 2
name.head  //It will give the first element of the list
name.tail  // returns the tail of a list (which includes everything except the head)

// COMMAND ----------

// DBTITLE 1,Collections : Set
// Set allows us to create a non-repeated group of entities
val uniqueNames = Set("Arthur", "Uther", "Mordred", "Vortigern", "Arthur", "Uther")
uniqueNames.contains("Uther1") // To check if an element exists in set
// We can add elements to a Set using the + method (which takes varargs i.e. variable-length arguments)
uniqueNames + ("Prince","Raj")
// we can remove elements using the - method
uniqueNames - "Raj"

// COMMAND ----------

// DBTITLE 1,Collections: Map
val empSalary = Map("Prince"->100000,"Manish"->80000,"Saurabh"->75000)
// Values for a specific key in map can be accessed as:
empSalary("Manish")
// We can add an entry to Map using the + method:
empSalary + ("Prem"->150000)
// To modify an existing mapping, we simply re-add the updated key-value:
empSalary + ("Prem"->120000)
// Note that since the collection is immutable, each edit operation returns a new collection( res0, res1) with the changes applied. The original collection 'empSalary' remains unchanged.

// COMMAND ----------

def scalaExample{  
    println("Hello Scala")  
}
scalaExample

// COMMAND ----------

