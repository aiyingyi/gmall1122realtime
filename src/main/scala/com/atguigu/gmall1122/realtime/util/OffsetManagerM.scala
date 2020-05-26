package com.atguigu.gmall1122.realtime.util

import java.util

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition
import redis.clients.jedis.Jedis

object OffsetManagerM {


  /**
    * 从Mysql中读取偏移量
    * @param groupId
    * @param topic
    * @return
    */
  def getOffset(groupId:String,topic:String):Map[TopicPartition,Long]={
    // 建立空的map集合，保存偏移量
      var offsetMap=Map[TopicPartition,Long]()

      val offsetJsonObjList: List[JSONObject] = MysqlUtil.queryList("SELECT  group_id ,topic,partition_id  , topic_offset  FROM offset_1122 where group_id='"+groupId+"' and topic='"+topic+"'")

      if(offsetJsonObjList!=null&&offsetJsonObjList.size==0){
            null
      }else {

            val kafkaOffsetList: List[(TopicPartition, Long)] = offsetJsonObjList.map { offsetJsonObj  =>
             (new TopicPartition(offsetJsonObj.getString("topic"),offsetJsonObj.getIntValue("partition_id")), offsetJsonObj.getLongValue("topic_offset"))
           }
           kafkaOffsetList.toMap
      }
   }

}
