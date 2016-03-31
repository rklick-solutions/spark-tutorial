package com.tutorial.graphX.context

import java.util.UUID

/**
  * Created by ved on 31/3/16.
  */
trait GraphHelper extends Serializable {

  //val PUBLISH_DATE = "publishDate"

  /**
    *
    * @param map
    * @param key
    * @return
    */
  def getValue(map: Map[String, String], key: String): String = {
    map.get(key) match {
      case Some(value) => value
      case None => ""
    }
  }

  /*/**
    *
    * @param vDf
    * @param col
    * @return
    */
  def getNodes(vDf: DataFrame, col: String, table: String): RDD[Nodes] = {
    vDf.map {
      case row =>
        val label = row.getAs[Any](col).toString
        val timestamp = /*row.getAs[Timestamp](PUBLISH_DATE).getTime*/ new Date().getTime
        Nodes(label.hashCode.toString, Style(label), timestamp)
    }
  }
*/

 /* /**
    *
    * @param nodes
    * @param levelMap
    * @param df
    * @return
    */
  def mergeNode(nodes: RDD[Nodes], levelMap: Map[String, String],
                df: DataFrame, table: String): RDD[Nodes] = {
    @tailrec
    def innerMerge(result: RDD[Nodes], levelMap: Map[String, String]): RDD[Nodes] = {
      levelMap.isEmpty match {
        case true => result
        case false =>
          val (key, value) = levelMap.head
          df.printSchema()
          val query = s"select distinct ${value} from ${table}"
          val mDf = df.sqlContext.sql(query)
          val data = getNodes(mDf, value, table)
          df.sqlContext.dropTempTable(table)
          innerMerge(data.union(result), levelMap - key)
      }
    }
    innerMerge(nodes, levelMap)
  }

  /**
    *
    * @param links
    * @param levelMap
    * @param graph
    * @return
    */
  def mergeLink(links: RDD[Links], levelMap: Map[String, String],
                graph: Graph[(String, Long, Map[String, String]), String]): RDD[Links] = {
    @tailrec
    def innerMerge(result: RDD[Links], levelMap: Map[String, String]): RDD[Links] = {
      levelMap.isEmpty match {
        case true => result
        case false =>
          val key = levelMap.head._1
          val data = getLinks(graph, key)
          innerMerge(data.union(result), levelMap - key)
      }
    }
    innerMerge(links, levelMap)
  }

  /**
    *
    * @param graphs
    * @param key
    * @return
    */
  def getLinks(graphs: Graph[(String, Long, Map[String, String]), String], key: String): RDD[Links] = {
    graphs.triplets.map { case graph =>
      val value = graph.srcAttr._3.get(key).getOrElse("")
      Links(randomUUID, graph.dstId.toString, value.hashCode.toString, graph.srcAttr._2)
    }
  }*/

  /**
    * Get unique id
    *
    * @return
    */
  def randomUUID: String = {
    UUID.randomUUID().toString
  }


}
