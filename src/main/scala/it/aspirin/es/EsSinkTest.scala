package it.aspirin.es

import java.util

import it.aspirin.utils.FlinkUtils
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = FlinkUtils.getStreamEnv
    val sourceDataStream = addTextSource(env)
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost",9200))
    addEsSink(httpHosts, sourceDataStream)
    addConsoleSink(sourceDataStream)
    FlinkUtils.start(env)
  }

  /**
   * 添加文件源
   *
   * @param env 流处理环境
   * @return
   */
  def addTextSource(env: StreamExecutionEnvironment): DataStream[String] = {
    env.readTextFile("/Users/yulei/IdeaProjects/personal/LearningFlink/src/main/resources/emp.txt")
  }

  /**
   * sink到elasticsearch
   *
   * @param dataStream 要sink的数据
   */
  def addEsSink(httpHosts: util.ArrayList[HttpHost], dataStream: DataStream[String]): Unit = {

    val myEsSinkFunc = new ElasticsearchSinkFunction[String] {
      override def process(data: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //封装数据
        val dataSource = new util.HashMap[String,String]()
        dataSource.put("id", data)

        //创建index request,发送http请求
        val indexedSeq = Requests.indexRequest()
          .index("es_sink")
          .source(dataSource)

        //用indexer发送请求
        requestIndexer.add(indexedSeq)
      }
    }

    dataStream.addSink(new ElasticsearchSink.Builder[String](httpHosts, myEsSinkFunc).build())
  }

  def addConsoleSink(dataStream: DataStream[String]):Unit = {
    dataStream.print()
  }



}
