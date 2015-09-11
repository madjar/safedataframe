package com.axa.dil.safedataframe

import org.apache.spark.sql.DataFrame
import shapeless._
import shapeless.syntax.singleton._

trait CheckDataFrame[T] {
  def check(df: DataFrame): Either[String, Unit]
}

object CheckDataFrame {
  implicit def emptyDataFrame: CheckDataFrame[HNil] =
    new CheckDataFrame[HNil] {
      def check(df: DataFrame) = Right(())
    }

  implicit def columnDataFrame[N <: Witness, T <: HList]
  (implicit cdf: Lazy[CheckDataFrame[T]], w: N)
  : CheckDataFrame[N :: T] =
    new CheckDataFrame[N :: T] {

      def check(df: DataFrame) =
        cdf.value.check(df).right.flatMap(_ => checkThis(df))

      def checkThis(df: DataFrame): Either[String, Unit] = {
        println(df.dtypes.seq)
        val colName = w.value.toString
        if (!df.columns.contains(colName)) {
          Left(s"Column '$colName' not found among '${df.columns.mkString(", ")}'")
        } else {
          val col = df(colName)
          Right(())
        }
      }
    }
}

class SafeDataFrame[T <: HList](val df: DataFrame) extends AnyVal {
  def apply[U <: Witness](col: U)(implicit tt: BasisConstraint[U :: HNil, T]) = df(col.value.toString)
}

object SafeDataFrame {
  def apply[T <: HList](df: DataFrame, witness: T)(implicit cdf: CheckDataFrame[T]): Either[String, SafeDataFrame[T]] =
    cdf.check(df).right.map(_ => new SafeDataFrame[T](df))
}


object Hello {
  def main(args: Array[String]): Unit = {
    println("Hello, world!")
    val conf = new org.apache.spark.SparkConf()
    conf.setMaster("local[*]")
      .setAppName("SafeDataFrame")
    val sc = new org.apache.spark.SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val df = sqlContext.read.json("people.json")
    val sdf = SafeDataFrame(df, "name".witness :: HNil) match {
      case Right(x) => x
      case Left(e) => sys.error(e)
    }

    sdf.df.filter(df("age") > 18).select("name")

    sdf("name".witness)
    sdf("notacolumn".witness)  // This fails to compile !

    sc.stop()
  }
}
