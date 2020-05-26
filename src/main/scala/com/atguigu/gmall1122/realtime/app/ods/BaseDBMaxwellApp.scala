package com.atguigu.gmall1122.realtime.app.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.atguigu.gmall1122.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object BaseDBMaxwellApp {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ods_base_db_maxwell_app")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val topic ="ODS_DB_GMALL1122_M";
    val groupId="base_db_maxwell_group"
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId,topic)
    var inputDstream: InputDStream[ConsumerRecord[String, String]]=null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if(offset!=null&&offset.size>0){
      inputDstream = MyKafkaUtil.getKafkaStream(topic,ssc,offset,groupId)
      //startInputDstream.map(_.value).print(1000)
    }else{
      inputDstream  = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }
    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] =null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val dbJsonObjDstream: DStream[JSONObject] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      val jsonObj: JSONObject = JSON.parseObject(jsonString)
      jsonObj
    }
    dbJsonObjDstream.foreachRDD{rdd=>
      rdd.foreachPartition { jsonObjItr =>
        for (jsonObj <- jsonObjItr) {
          val dataObj: JSONObject = jsonObj.getJSONObject("data")
          val tableName = jsonObj.getString("table")
          val id = dataObj.getString("id")
          val topic = "ODS_T_" + tableName.toUpperCase
          if (dataObj != null && !dataObj.isEmpty && !jsonObj.getString("type").equals("delete"))
            if ((tableName == "order_info" && jsonObj.getString("type").equals("insert"))
              || (tableName == "base_province")
              ||(tableName == "user_info")
              ||(tableName == "spu_info")
              ||(tableName == "sku_info")
              ||(tableName == "base_category3")
              ||(tableName == "base_trademark")
              ||((tableName == "order_detail") )
            ) {
              if(tableName == "order_info"|| tableName == "order_detail" ){
                  Thread.sleep(300)
              }
              MyKafkaSink.send(topic, id, dataObj.toJSONString)
            }
        }
      }
      OffsetManager.saveOffset(groupId,topic,offsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
