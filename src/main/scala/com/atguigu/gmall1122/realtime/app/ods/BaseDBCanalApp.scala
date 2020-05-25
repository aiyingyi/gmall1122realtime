package com.atguigu.gmall1122.realtime.app.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall1122.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

class BaseDBCanalApp {
}

/**
 * 将canal提交到kafka中的数据采用流的方式，读取并提取出数据部分，然后根据表格写入到不同的topic中
 */
object BaseDBCanalApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("base_db_canal_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val topic = "ODS_DB_GMALL1122_C";
    val groupId = "base_db_canal_group"

    // 假如宕机，则从redis中获取offset，实现断点消费
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)
    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 如果offset并不为空，说明不是第一次消费，直接根据offset创建
    if (offset != null && offset.size != 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      // 第一次创建流，直接创建
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }
    // 取得偏移量
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    // 将流中的数据转换成JSON对象
    val dbJsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map(
      record => {
        val jsonString: String = record.value()
        val jsonObj: JSONObject = JSON.parseObject(jsonString)
        jsonObj
      }
    )
    // 对数据进行消费，采用行动算子，同时注意提交offset
    dbJsonObjDstream.foreachRDD {
      // 先消费RDD
      rdd => {rdd.foreachPartition { josnObjItr => {
        for (jsonObj <- josnObjItr) {
          val dataArr: JSONArray = jsonObj.getJSONArray("data")
          for (i <- 0 until dataArr.size()) {
            val dataJsonObj: JSONObject = dataArr.getJSONObject(i)
            // 根据表格建立topics
            val topic = "ODS_T_" + jsonObj.getString("table").toUpperCase
            val id: String = dataJsonObj.getString("id")
            MyKafkaSink.send(topic, id, dataJsonObj.toJSONString)
          }
        }
      }
      }
    }
        // 消费完成后提交offset
      OffsetManager.saveOffset(groupId, topic, offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
