package com.atguigu.gmall1122.realtime.app

import java.lang
import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1122.realtime.bean.DauInfo
import com.atguigu.gmall1122.realtime.util.{MyEsUtil, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/*
  统计日活跃设备
 */
class DauApp {


}
object DauApp{
  def main(args: Array[String]): Unit = {

    // 本地调试，选择local[*],发布到集群上执行时，指定参数，会覆盖掉代码中的参数
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val groupId = "GMALL_DAU_CONSUMER"
    val topic = "GMALL_START"

    // 获取起始读取的offset
    val startOffset: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)

    var startInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    /*
        假如第一次从kafka中读取数据，redis还没有保存偏移量，那就先不通过偏移量读取，等第二次读取时再获取offset
     */
    if(startOffset != null && startOffset.size>0){
      startInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, startOffset,groupId)
      startInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, startOffset,groupId)
    }else{
      startInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }


    //获得本批次偏移量的移动后的新位置
    // 通过 DStream获取偏移量，类型  Array[OffsetRange]，OffsetRange有一个fromOffset和一个UntilOffset，我们主要需要UntilOffset
    var startupOffsetRanges: Array[OffsetRange] =null
    // offsetRanges 是一个数组，下标不是对应的分区号
    val startupInputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = startInputDstream.transform { rdd =>
      // 将流中的rdd强转为HasOffsetRanges类型，并获取Array[OffsetRange]
      startupOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      // 将原来的数据返回
      rdd
    }


    // 将流中的消息转换成json对象
    val startJsonObjDstream: DStream[fastjson.JSONObject] = startupInputGetOffsetDstream.map(
      record => {
        val jsonString: String = record.value()
        val jsonObject: fastjson.JSONObject = JSON.parseObject(jsonString)
        jsonObject
      }
    )
    // 去重写入，每天将日活写入一个清单，保存在redis中，因此，每天一个key，数据类型应该选择set，实现去重
    /*
        注意：按照分区来对数据进行操作时，不能直接返回迭代器对象，因为迭代器对象迭代之后，数据就是空的了
     */
    val startJsonObjWithDauDstream: DStream[fastjson.JSONObject] = startJsonObjDstream.mapPartitions(jsonObjIter => {
      // 从连接池中获取redis连接
      val jedis = RedisUtil.getJedisClient
      val list: List[fastjson.JSONObject] = jsonObjIter.toList

      // 创建一个新的集合，保存本批次中第一次启动的设备
      val jsonObjFilteredList = ListBuffer[JSONObject]()
      for (jsonObj <- list) {
        // 获取消息中的时间戳
        val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date(jsonObj.getLong("ts")))
        val key = "dau:" + dateStr
        // 将设备的mid写入对应的set中,同时获取是否插入成功的反馈
        // redis插入成功返回1，失败返回0
        val isFirst: lang.Long = jedis.sadd(key, jsonObj.getJSONObject("common").getString("mid"))
        // 如果是第一次插入，那么将对象保存
        if(isFirst == 1L){
          jsonObjFilteredList += jsonObj
        }
      }
      // close()会判断连接是否从连接池中获取。是，则不会关闭连接，还给连接池。
      jedis.close()
      // 将分区中第一次启动的设备进行返回，这样实现了过滤功能
      jsonObjFilteredList.toIterator

    })
    // 将过滤好的数据插入到es中的gmall1122_dau_info*  索引中
    // 1. 转换数据结构，采用样例类封装
    val dauInfoDstream: DStream[DauInfo] = startJsonObjWithDauDstream.map(jsonObj => {
      val commonObj: JSONObject = jsonObj.getJSONObject("common")
      val commonJsonObj: JSONObject = jsonObj.getJSONObject("common")
      val dateTimeStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new Date(jsonObj.getLong("ts")))
      val dateTimeArr: Array[String] = dateTimeStr.split(" ")
      val dt: String = dateTimeArr(0)
      val timeArr: Array[String] = dateTimeArr(1).split(":")
      val hr = timeArr(0)
      val mi = timeArr(1)
      // 将封装的样例类对象返回
      DauInfo(commonJsonObj.getString("mid"),
        commonJsonObj.getString("uid"),
        commonJsonObj.getString("ar"),
        commonJsonObj.getString("ch"),
        commonJsonObj.getString("vc"),
        dt, hr, mi, jsonObj.getLong("ts")
      )
    })

    // 保存在es中,要使用行动算子
    dauInfoDstream.foreachRDD {
      rdd =>rdd.foreachPartition { dauInfoItr => {
          // 将mid取出作为es中的主键
          val dataList: List[(String, DauInfo)] = dauInfoItr.toList.map(dauInfo => (dauInfo.mid, dauInfo))

          // 利用当前日期，定义index
          val dt = new SimpleDateFormat("yyyyMMdd").format(new Date())
          val indexName = "gmall1122_dau_info_" + dt
          // 批量写入。Bulk是jest的API，可以封装多个index操作
          MyEsUtil.saveBulk(dataList, indexName)
        }
        }
        // 消费完成后，进行偏移量的提交
        OffsetManager.saveOffset(groupId,topic,startupOffsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}