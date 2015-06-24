package com.growing.common

import java.io.{IOException, FileInputStream, InputStreamReader, File}
import java.sql.{Connection, ResultSet, DriverManager}
import java.util.Properties
import scala.collection.JavaConversions._

import org.apache.spark.SparkException


class AppContext(args: Array[String]) {

  val properties: Map[String, String] = {

    if(args.length < 1) {
      System.err.println("Usage args FILE")
      System.exit(1)
    }
    val filename = args(0)
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().map(k => (k, properties(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  def executeSql(sql: String) (f: ResultSet => Unit) = {
    Class.forName(properties.getOrElse("driver", "org.postgresql.Driver"))
    val url = properties.getOrElse("url", "jdbc:postgresql://localhost:7531/rules")
    val user = properties.getOrElse("user", "test")
    val password = properties.getOrElse("password", "test")

    var connection: Connection = null
    try {
      connection = DriverManager.getConnection(url, user, password)
      val res = connection.createStatement().executeQuery(sql)
      f(res)
    } catch {
      case e: Exception => throw new Exception(e)
    } finally {
      connection.close()
    }
  }
}
