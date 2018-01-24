package com.rejispool.sparklessons;

/**
  * Created by reji on 24/01/18.
  */
object DataJoinOperation{
  Logger.getLogger("org").setLevel(Level.ERROR)
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]","DataJoinOperation")

    println("Printing EMP details details\n----------------------")

    val emp_empId  =sc.textFile("input/datajoin/EMPID_EMPDETAILS.txt")
    val emp_empId_lineRDD = emp_empId.flatMap( e => e.split("\n"))
    //emp_empId_lineRDD.collect().foreach(println)
    val emp_empId_lineRDD_tuple = emp_empId_lineRDD.map(emp => (emp.split(",")(0),emp.split(",")(1),emp.split(",")(2),emp.split(",")(3)))
    //emp_empId_lineRDD_tuple.foreach(println)
    val emp_empId_lineRDD_tuple_pairrdd=emp_empId_lineRDD_tuple.map(tupleemp => (tupleemp._1,(tupleemp._1,tupleemp._2,tupleemp._3,tupleemp._4)))
    emp_empId_lineRDD_tuple_pairrdd.foreach(println)


    println("\n\n----------------------")
    println("Printing LOC details\n----------------------")
    val emp_loc_details = sc.textFile("input/datajoin/EMP_LOC_DETAILS.txt")
    val emp_loc_details_lineRDD = emp_loc_details.flatMap( l => l.split("\n"))
    val emp_loc_details_lineRDD_tuple = emp_loc_details_lineRDD.map( line => (line.split(",")(0),line.split(",")(1),line.split(",")(2),line.split(",")(3)))
    //emp_loc_details_lineRDD.foreach(println)
    //emp_loc_details_lineRDD_tuple.foreach(println)
    val emp_loc_details_lineRDD_tuple_pairrdd=emp_loc_details_lineRDD_tuple.map(tuple => (tuple._1,(tuple._1,tuple._2,tuple._3,tuple._4)))
    emp_loc_details_lineRDD_tuple_pairrdd.foreach(println)

    //Joining 2 RDDs
    println("\n\n----------------------")
    println("Joined dataset")
    println("----------------------")
    val allJoin = emp_empId_lineRDD_tuple_pairrdd.leftOuterJoin(emp_loc_details_lineRDD_tuple_pairrdd).collect()
    allJoin.foreach(println)


    val selectiveDisplay = allJoin.map(x=>(x._1,x._2._1._2,x._2._1._3,x._2._1._4,x._2._2))
    selectiveDisplay.foreach(println)
  }
}
