package com.lakala.finance.anti_fraud.test

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import scala.xml.{Elem, XML}

/**
  * Created by longxiaolei on 2017/8/8.
  */
object TestJDBC {
  def main(args: Array[String]) {
    val confXml: Elem = XML.load(args(0))
    println(s"完成加载配置文件")

    val mysql_url = (confXml \ "env" \ "mysql" \ "url").text
    val mysql_user = (confXml \ "env" \ "mysql" \ "user").text
    val mysql_passwd = (confXml \ "env" \ "mysql" \ "passwd").text

    classOf[com.mysql.jdbc.Driver]
    println(s"url:${mysql_url},uer:${mysql_user},")
    val con: Connection = DriverManager.getConnection(mysql_url, mysql_user, mysql_passwd)

    val ps: PreparedStatement = con.prepareStatement("select count(1) from exception_orderno_tbl")
    val query: ResultSet = ps.executeQuery()

    query.next()
    val int: Int = query.getInt(1)
    print(int)
  }
}
