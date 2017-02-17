package com.finupgroup.cif.sql

import com.finupgroup.cif.sql.AST._
import org.apache.commons.lang3.time.DateUtils

import scala.util.matching.Regex
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.input.CharArrayReader.EofCh


/**
 * Created by jadetang on 15-3-22.
 */
object Parser extends StandardTokenParsers {


  class SqlLexical extends StdLexical {

    case class FloatLit(chars: String) extends Token {
      override def toString = chars
    }

    /* case class Ident(chars:String) extends Token{
       override def toString = chars
     }*/

    override def token: Parser[Token] =
      (identChar ~ rep(identChar | digit) ^^ { case first ~ rest => processIdent(first :: rest mkString "") }
        | rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
        case i ~ None => NumericLit(i mkString "")
        case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
      }
        | '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ { case '\'' ~ chars ~ '\'' => StringLit(chars mkString "") }
        | '\"' ~ rep(chrExcept('\"', '\n', EofCh)) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
        | EofCh ^^^ EOF
        | '\'' ~> failure("unclosed string literal")
        | '\"' ~> failure("unclosed string literal")
        | delim
        | failure("illegal character")
        )

    def regex(r: Regex): Parser[String] = new Parser[String] {
      def apply(in: Input) = {
        val source = in.source
        val offset = in.offset
        val start = offset // handleWhiteSpace(source, offset)
        (r findPrefixMatchOf (source.subSequence(start, source.length))) match {
          case Some(matched) =>
            Success(source.subSequence(start, start + matched.end).toString,
              in.drop(start + matched.end - offset))
          case None =>
            Success("", in)
        }
      }
    }
  }

  override val lexical = new SqlLexical
  val functions = Seq("count", "sum", "avg", "min", "max", "substring", "extract")

  lexical.reserved +=(
    "select", "as", "or", "and", "group", "order", "by", "where", "limit",
    "join", "asc", "desc", "from", "on", "not", "having", "distinct",
    "case", "when", "then", "else", "end", "for", "from", "exists", "between", "like", "in",
    "year", "month", "day", "null", "is", "date", "interval", "group", "order",
    "date", "left", "right", "outer", "inner"
    )

  lexical.reserved ++= functions

  lexical.delimiters +=(
    "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")", ",", ".", ";"
    )


  def floatLit: Parser[String] =
    elem("decimal", x => x.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)

  def literal: Parser[SqlExpr] = {
    println("kankan111")
    numericLit ^^ { case i => Literal(i.toInt) } |
      stringLit ^^ { case s => Literal(s.toString) } |
      floatLit ^^ { case f => Literal(f.toDouble) } |
      "null" ^^ (_ => Literal(null))|
      "date" ~> stringLit ^^ { case d =>
        println(s"d::::::::::::::::::$d")
//        val format =  new SimpleDateFormat("yyyy-MM-dd")
//        val date = format.parse(d.trim)

        Literal(DateUtils.parseDate(d,"yyyy-MM-dd"))
      }|
      "interval" ~> stringLit ^^ { case x =>
        println(x)
        Literal(null)
      }
  }

  def fieldIdent: Parser[SqlExpr] = {
    ident ~ opt("." ~> ident) ^^ {
      case table ~ Some(b: String) => FieldIdent(Option(table), b)
      case column ~ None => FieldIdent(None, column)
    }
  }

  //override def ident:Parser[String] = elem("ident",x=>x.isInstanceOf[lexical.Ident]&&(!(x.chars.contains("."))))  ^^ (_.chars)
  /*
    override def ident: Parser[String] =
      elem("identifier", _.isInstanceOf[scala.util.parsing.combinator.token.Tokens.Identifier]) ^^ (_.chars)*/

  def primaryWhereExpr: Parser[SqlExpr] = {
    (literal | fieldIdent) ~ ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") ~ (literal | fieldIdent) ^^ {
      case lhs ~ "=" ~ rhs => Eq(lhs, rhs)
      case lhs ~ "<>" ~ rhs => Neq(lhs, rhs)
      case lhs ~ "!=" ~ rhs => Neq(lhs, rhs)
      case lhs ~ "<" ~ rhs => Ls(lhs, rhs)
      case lhs ~ "<=" ~ rhs => LsEq(lhs, rhs)
      case lhs ~ ">" ~ rhs => Gt(lhs, rhs)
      case lhs ~ ">=" ~ rhs => GtEq(lhs, rhs)
    } | "(" ~> expr <~ ")"
  }

  def andExpr: Parser[SqlExpr] =
    primaryWhereExpr * ("and" ^^^ { (a: SqlExpr, b: SqlExpr) => And(a, b) })

  def orExpr: Parser[SqlExpr] =
    andExpr * ("or" ^^^ { (a: SqlExpr, b: SqlExpr) => Or(a, b) })

  def expr: Parser[SqlExpr] = orExpr

  def whereExpr: Parser[SqlExpr] = "where" ~> expr

  def projectionStatements: Parser[Seq[Projection]] = repsep(projection, ",")

  def projection: Parser[Projection] = {
    "*" ^^ (_ => Projection(StarProj(), None)) |
      primarySelectExpr ~ opt("as" ~> ident) ^^ {
        case expr ~ alias => Projection(expr, alias)
      }
  }

  def selectLiteral: Parser[SqlProj] = {
    numericLit ^^ { case i => Literal(i.toInt) } |
      stringLit ^^ { case s => Literal(s.toString) } |
      floatLit ^^ { case f => Literal(f.toDouble) } |
      "null" ^^ (_ => Literal(null))
  }


  def selectIdent: Parser[SqlProj] = {
    ident ~ opt("." ~> ident) ^^ {
      case table ~ Some(b: String) => FieldIdent(Option(table), b)
      case column ~ None => FieldIdent(None, column)
    }
  }

  def primarySelectExpr: Parser[SqlProj] = {
    knowFunction | selectLiteral | selectIdent
  }

  def knowFunction: Parser[SqlProj] = {
    def singeSelectExpr: Parser[SqlProj] = {
      selectLiteral | selectIdent
    }
    "count" ~> "(" ~> ("*" ^^ (_ => CountStar()) | opt("distinct") ~ singeSelectExpr ^^ { case d ~ e => CountExpr(e, d.isDefined) }) <~ ")" |
      "min" ~> "(" ~> singeSelectExpr <~ ")" ^^ (Min(_)) |
      "max" ~> "(" ~> singeSelectExpr <~ ")" ^^ (Max(_)) |
      "sum" ~> "(" ~> (opt("distinct") ~ singeSelectExpr) <~ ")" ^^ { case d ~ e => Sum(e, d.isDefined) } |
      "avg" ~> "(" ~> (opt("distinct") ~ singeSelectExpr) <~ ")" ^^ { case d ~ e => Avg(e, d.isDefined) }
  }

  def select: Parser[SelectStmt] = "select" ~> projectionStatements ~ fromStatements ~ opt(groupStatements) ~ opt(whereExpr) ~ opt(orderByExpr) ~ opt(limit) ~ opt(";") ^^ {
    case p ~ f ~ g ~ w ~ o ~ l ~ end => println(f);SelectStmt(p, f, w, g, o, l)
  }

  def orderByExpr: Parser[SqlOrderBy] = "order" ~> "by" ~> rep1sep(selectIdent, ",") ^^ {
    case keys => SqlOrderBy(keys)
  }

  def limit: Parser[(Option[Int], Int)] = {
    "limit" ~> opt(numericLit <~ ",") ~ numericLit ^^ {
      case None ~ size => (None, size.toInt)
      case Some(b:String) ~ size =>(Some(b.toInt),size.toInt)}
  }

  def fromStatements: Parser[SqlRelation] = "from" ~> relations

  def parse(sql: String): Option[SelectStmt] = phrase(select)(new lexical.Scanner(sql)) match {
    case Success(r, q) => Option(r)
    case x => throw new IllegalArgumentException(x.toString);
  }

  def relations: Parser[SqlRelation] = simple_relation

  def simple_relation: Parser[SqlRelation] = small_relation ~ opt("as") ~ opt(ident) ^^ {
    case tablename ~ _ ~ alias =>
      TableRelationAST(tablename, alias)
  }

  def small_relation: Parser[String] =  opt(ident) ~ opt(".") ~ opt(ident) ^^ {
    case dn ~ po ~ tn =>
      if (dn.getOrElse("") != "" && po.getOrElse("") == "." && tn.getOrElse("") !="") {
        dn.getOrElse("") + po.getOrElse("") + tn.getOrElse("")
      } else if (dn.getOrElse("") == "" && tn.getOrElse("") == "") {
        throw new IllegalArgumentException("DBName & TableName is empty")
      } else if (dn.getOrElse("") == "" && po.getOrElse("") == ".") {
        throw new IllegalArgumentException("DBName is empty")
      } else if (tn.getOrElse("") == "" && po.getOrElse("") == ".") {
        throw new IllegalArgumentException("TableName is empty")
      } else {
        dn.getOrElse("")
      }
  }

  def groupStatements: Parser[SqlGroupBy] = "group" ~> "by" ~> rep1sep(selectIdent, ",") ^^ {
    case keys => SqlGroupBy(keys)
  }




}
