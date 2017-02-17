package com.finupgroup.cif.sql

import com.finupgroup.cif.sql.MetaData.MetaData


object AST {

//  class IllegalSqlException( message:String) extends Exception

  trait Node

  trait SqlExpr extends Node

  trait Binop extends SqlExpr {
    val lhs: SqlExpr
    val rhs: SqlExpr
  }

  case class Literal(value: MetaData[_]) extends SqlExpr with SqlProj

  case class FieldIdent(qualify: Option[String], name: String) extends SqlExpr with SqlProj

  case class Or(lhs: SqlExpr, rhs: SqlExpr) extends Binop

  case class And(lhs: SqlExpr, rhs: SqlExpr) extends Binop

  trait EqualityLike extends Binop

  case class Eq(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike

  case class Neq(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike

  case class Ls(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike

  case class Gt(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike

  case class LsEq(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike

  case class GtEq(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike


  //case class StarExpr() extends SqlProj


  sealed trait SqlProj extends Node

  trait SqlAgg extends Node


  case class StarProj() extends SqlProj

  // case class FiledProj(table:String,column:String,alias:String) extends SqlProj
  case class Projection(sqlProj: SqlProj, alias: Option[String]) extends Node

  case class CountStar() extends SqlProj with SqlAgg

  case class CountExpr(expr: SqlProj, distinct: Boolean) extends SqlProj with SqlAgg

  case class Sum(expr: SqlProj, distinct: Boolean) extends SqlProj with SqlAgg

  case class Avg(expr: SqlProj, distinct: Boolean) extends SqlProj with SqlAgg

  case class Min(expr: SqlProj) extends SqlProj with SqlAgg

  case class Max(expr: SqlProj) extends SqlProj with SqlAgg

  case class SelectStmt(projections: Seq[Projection],
                        relations: SqlRelation,
                        where: Option[SqlExpr],
                        groupBy: Option[SqlGroupBy],
                        orderBy: Option[SqlOrderBy],
                        limit: Option[(Option[Int],Int)]) extends Node

  trait SqlRelation extends Node

  case class TableRelationAST(name: String, alias: Option[String]) extends SqlRelation

  case class SqlGroupBy(keys:Seq[SqlProj]) extends Node

  case class SqlOrderBy(keys:Seq[SqlProj]) extends Node

  /*trait SqlExpr[+T] {
    def eval: T
  }

  trait BiOp[T] extends SqlExpr[T]

  abstract case class LogicalOp(left:SqlExpr[Any],right:SqlExpr[Any]) extends BiOp[Boolean]

  case class Equal(override val left:SqlExpr[Any],override val right:SqlExpr[Any]) extends LogicalOp(left,right) {
    override def eval: Boolean = {
      (left, right) match {
        case (LiteralExpress(_), LiteralExpress(_)) => left.eval == right.eval
        case (LogicalOp(_,_), LogicalOp(_,_)) => left.asInstanceOf[LogicalOp].eval && right.asInstanceOf[LogicalOp].eval

      }
    }
  }

  abstract case class LogicExpress extends SqlExpr[Boolean] {
    override def eval: Boolean
  }

  case class LiteralExpress[T](value: T) extends SqlExpr[T] {
    override def eval: T = value
  }

   case class Equal(val left: SqlExpr[Any], val right: SqlExpr[Any]) extends LogicExpress {
     override def eval: Boolean = {
       (left, right) match {
         case (LiteralExpress, LiteralExpress) => left.eval == right.eval
         case (LogicExpress, LogicExpress) => left.eval.asInstanceOf[Boolean] && right.eval.asInstanceOf[Boolean]
       }
     }
   }


  case class SelectColumn(name: String) {
    //    override def toString() :String = name+"bbb"
  }

  def main(args: Array[String]) {
    val literalExpress = LiteralExpress("name")
    val literalExpress2 = LiteralExpress("name")
    val literalExpress3 = LiteralExpress("notName")
    val equal2 = Equal(literalExpress2, literalExpress3)
    val equal3 = Equal(literalExpress, literalExpress2)


    val equal = Equal(equal2, equal3)
    println(equal.eval)
  }*/

}
