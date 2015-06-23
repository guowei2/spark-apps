package com.growing

import java.sql.DriverManager

import kafka.serializer.StringDecoder
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.JoinedRow4
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.hive._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


object Kafka2Spark {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("kakfa2spark"))

    val brokers = "kafka-0:9092,kafka-1:9092"
    val topic = "vds-pc-web,vds-pc-mobile"

    val sql_visit = """
                      |INSERT INTO TABLE liepin.visit partition (day)
                      |SELECT `_id` ,`ai` ,`av` ,`b` ,`bw` ,`bwv` ,`cc` ,`city` ,`countrycode` ,`countryname` ,
                      |`d` ,`grwng_uid` ,`ip` ,`os` ,`osv` ,`p` ,`rd` ,`region` ,
                      |`rf` ,`s` ,`sh` ,`stm` ,`sw` , `t` ,`tm`, `u` ,`ua` ,`vc` ,
                      |from_unixtime(cast(substr(stm,0,10) AS int),"yyyyMMdd")
                      |from vds
                      |where t = 'vst'
                    """.stripMargin

    val sql_page = """
                     |INSERT INTO TABLE liepin.page partition (day, time)
                     |SELECT `_id` ,`city` ,`countrycode` ,`countryname` ,
                     |`d` ,`grwng_uid` ,`ip` , `p` , `pt` ,`q` ,`rd` ,`region` ,
                     |`rf` ,`s` ,`stm`  , `t` ,`tm` , `u` ,`ua` ,`visit_id` ,
                     |from_unixtime(cast(substr(stm,0,10) AS int),"yyyyMMdd") ,
                     |substr(from_unixtime(cast(substr(stm,0,10) AS int),"HHmm"),0,3)
                     |from vds
                     |where t = 'page'
                   """.stripMargin

    val sql_action = """
                       |SELECT `_id` ,`c` ,`d` ,`gi` ,
                       |`h` ,`i` ,`idx` , `p` , `page_id` ,`q` ,
                       |`s` ,`stm` , `t` ,`tm` , `u` ,`v` ,`visit_id` , `x`
                       |from vds
                       |where t != 'vst' and t != 'page'
                     """.stripMargin

    val sql_action_total = """
                             |INSERT INTO TABLE liepin.action_total partition (day, time)
                             |SELECT `_id` ,`c` ,`d` ,`gi` ,
                             |`h` ,`i` ,`idx` , `p` , `page_id` ,`q` ,
                             |`s` ,`stm` , `t` ,`tm` , `u` ,`v` ,`visit_id` , `x`,
                             |from_unixtime(cast(substr(stm,0,10) AS int),"yyyyMMdd") ,
                             |substr(from_unixtime(cast(substr(stm,0,10) AS int),"HHmm"),0,3)
                             |from ac
                           """.stripMargin


    val fields = Seq("_id" ,"ai" ,"av" ,"b" ,"bw" ,"bwv" ,"c" ,"cc" ,"city" ,"countrycode" ,"countryname" ,
      "d" ,"gi" ,"grwng_uid" ,"h" ,"i" ,"idx" , "ip" ,"os" ,"osv" ,"p" , "page_id", "pt" ,"q" ,"rd" ,"region" ,
      "rf" ,"s" ,"sh" ,"stm" ,"sw" , "t" ,"tm" , "u" ,"ua"  ,"v","vc" ,"visit_id" , "x")

    val fields_action_tag = Seq("id" ,"c" ,"d" ,"gi" ,"h" ,"i" ,"idx" ,"p" , "page_id" ,"q"
      ,"s" ,"stm" , "t" ,"tm" , "u" ,"v","visit_id" , "x", "tag_id", "tag_name")

    val sql_action_tag = """
                           |INSERT INTO TABLE liepin.action_total partition (day, time)
                           |SELECT `_id` ,`c` ,`d` ,`gi` ,
                           |`h` ,`i` ,`idx` , `p` , `page_id` ,`q` ,
                           |`s` ,substr(stm,0,10) , `t` ,`tm` , `u` ,`v` ,`visit_id` , `x`, tag_id, tag_name
                           |from_unixtime(cast(substr(stm,0,10) AS int),"yyyyMMdd") ,
                           |substr(from_unixtime(cast(substr(stm,0,10) AS int),"HHmm"),0,3)
                           |from ac
                         """.stripMargin

    val ssc = new StreamingContext(sc, Seconds(10))
    val hc = new HiveContext(sc)

    case class Rule (tp: String, col: Int, v: String)
    case class Tag (id: String, name: String)

    abstract class Node extends TreeNode[Node] {
      self: Node with Product =>
    }
    case class MidNode (rule: Rule, children: mutable.Buffer[Node]) extends Node

    case class LeafNode (rule: Rule, tag: Tag, children: mutable.Buffer[Node] = mutable.Buffer.empty) extends Node

    val ruleTree: List[Node] = {

      val start_time = System.currentTimeMillis()
      val sql_rule = "select d,p,q,t,x,v,uid,name from test order by d,p,q,x,v,t"
      Class.forName("org.postgresql.Driver")
      val connection = DriverManager.getConnection("jdbc:postgresql://pgdb:7531/rules","apps","O|EUc8uQ:>F33TyJ")
      val res = connection.createStatement().executeQuery(sql_rule)

      val root = mutable.Buffer[Node]()
      while (res.next()) {

        var currentNode = root
        var findLeaf = false
        val rules = ArrayBuffer[Rule]()

        Seq(2, 7, 9, 12, 17, 15).zipWithIndex.reverse.foreach { case (s, i) =>
          val col = res.getString(i + 1)
          if (col == null || col == "") {
            if (findLeaf) rules.append(Rule("", s, ""))
          } else {
            val index = col.indexOf(":")
            if( index == -1) throw new Exception("error rule format")
            findLeaf = true
            rules.append(Rule(col.substring(0,index), s, col.substring(index+1)))
          }
        }

        val ruleIter = rules.reverseIterator
        ruleIter.foreach { rule =>
          val child =
            if (ruleIter.hasNext) MidNode(rule, mutable.Buffer[Node]())
            else LeafNode(rule, Tag(res.getString(7), res.getString(8)))

          if (currentNode.isEmpty) currentNode.append(child)
          else {
            (currentNode.last, child) match {
              case (MidNode(Rule(tp1, _, v1), _), MidNode(Rule(tp2, _, v2), _)) =>
                if (tp1 != tp2 || v1 != v2) currentNode.append(child)
              case _ => currentNode.append(child)
            }
          }
          currentNode = currentNode.last match {
            case MidNode(_, children) => children
            case LeafNode(_,_,children) => children
          }
        }
      }
      println("find rule use:" + (System.currentTimeMillis() - start_time) + "ms")
//      println("rule tree: " + root.toList)
      root.toList
    }


    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers,"group.id" -> "spark")
    val vds = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topic.split(",").toSet).map(_._2)



    vds.foreachRDD { rdd =>
      if (rdd.partitions.length > 0) {
        val schema = StructType(fields.map(StructField(_, StringType)))

        hc.jsonRDD(rdd, schema).registerTempTable("vds")
        hc.sql(sql_visit)
        hc.sql(sql_page)
        hc.sql(sql_action).registerTempTable("ac")
        hc.sql(sql_action_total)

        def matchRule(rule: Rule, row: Row): Boolean = {

          try {
            rule.tp match {
              case "" => true
              case "=" => row.getString(rule.col) == rule.v
              case "wildcard" => rule.v.r.findFirstIn(row.getString(rule.col)).isDefined
              case "match_phrase" => " " + row.getString(rule.col) + " ".indexOf(" "+rule.v+" ") != -1
              case "in" =>
                rule.v.split(",").filter(_==row.getString(rule.col)).nonEmpty
              case "!=" => row.getDouble(rule.col) != rule.v.toDouble
              case ">" => row.getDouble(rule.col) > rule.v.toDouble
              case "<" => row.getDouble(rule.col) < rule.v.toDouble
              case ">=" => row.getDouble(rule.col) >= rule.v.toDouble
              case "<=" => row.getDouble(rule.col) <= rule.v.toDouble
              case op => throw new Exception(s"not support operator $op")
            }
          } catch {
            case e: Exception =>
              println(e.getMessage)
              false
          }

        }

        val ac = hc.sql("select * from ac").mapPartitions { iter =>

          val joinedRow = new JoinedRow4

          iter.flatMap { row=>
            val tags = ArrayBuffer[Tag]()
            var checkNodes = ruleTree
            while (checkNodes.nonEmpty) {
              val childNodes = ArrayBuffer[Node]()
              checkNodes.foldLeft((tags, childNodes)) {
                case ((t, c), node) => node match {
                  case MidNode(rule, child) if matchRule(rule, row) =>
                    (t, c ++= child)
                  case LeafNode(rule, tag, _) if matchRule(rule, row) =>
                    (t ++= Seq(tag), c)
                  case _ => (t, c)
                }
              }
              checkNodes = childNodes.toList
            }
            tags.map(t => joinedRow(row, Row(Seq(t.id, t.name))))
          }
        }

        hc.createDataFrame(ac, StructType(fields_action_tag.map(StructField(_, StringType)))).registerTempTable("act")
//        hc.sql("desc act").collect().foreach(println)
//        hc.sql("select * from act limit 10").collect().foreach(println)
        hc.sql(sql_action_tag)

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

}

