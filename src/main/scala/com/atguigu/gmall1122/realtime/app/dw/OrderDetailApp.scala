package com.atguigu.gmall1122.realtime.app.dw
import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1122.realtime.bean.{OrderDetail}
import com.atguigu.gmall1122.realtime.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderDetailApp {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dw_order_detail_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "ODS_T_ORDER_DETAIL";
    val groupId = "dw_order_detail_group"

    /////////////////////  偏移量处理///////////////////////////
    // 获取偏移量
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)
    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      // 第一次读取，还没有保存过偏移量
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    /////////////////////  业务处理///////////////////////////
    val orderDetailDstream: DStream[OrderDetail] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      // 将json转换成样例类，样例类的属性名必须与json的属性名完全一致
      val orderDetail: OrderDetail = JSON.parseObject(jsonString,classOf[OrderDetail])
      orderDetail
    }
    /////////////// 合并 商品信息////////////////////
    val orderDetailWithSkuDstream: DStream[OrderDetail] = orderDetailDstream.mapPartitions { orderDetailItr =>
      val orderDetailList: List[OrderDetail] = orderDetailItr.toList
      if(orderDetailList.size>0) {
        // 取出样例类中的所有查询条件的List，也就是join字段的所有的值
        val skuIdList: List[Long] = orderDetailList.map(_.sku_id)
        //  组合sql
        val sql = "select id ,tm_id,spu_id,category3_id,tm_name ,spu_name,category3_name  from gmall1122_sku_info  where id in ('" + skuIdList.mkString("','") + "')"
       // 查询出join的那张表的所有数据，注意：对于数据量比较大的表，不要广播，每个分区都查询一遍比较好
        val skuJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        // 将查询结果转换成map
        val skuJsonObjMap: Map[Long, JSONObject] = skuJsonObjList.map(skuJsonObj => (skuJsonObj.getLongValue("ID"), skuJsonObj)).toMap
        for (orderDetail <- orderDetailList) {
          // join操作
          val skuJsonObj: JSONObject = skuJsonObjMap.getOrElse(orderDetail.sku_id, null)
          orderDetail.spu_id = skuJsonObj.getLong("SPU_ID")
          orderDetail.spu_name = skuJsonObj.getString("SPU_NAME")
          orderDetail.tm_id = skuJsonObj.getLong("TM_ID")
          orderDetail.tm_name = skuJsonObj.getString("TM_NAME")
          orderDetail.category3_id = skuJsonObj.getLong("CATEGORY3_ID")
          orderDetail.category3_name = skuJsonObj.getString("CATEGORY3_NAME")
        }
      }
      // 返回新的对象Iterator
      orderDetailList.toIterator
    }

    orderDetailWithSkuDstream.cache()

    orderDetailWithSkuDstream.print(1000)



         //写入es
    //   println("订单数："+ rdd.count())
    orderDetailWithSkuDstream.foreachRDD{rdd=>
        rdd.foreachPartition{orderDetailItr=>
          val orderDetailList: List[OrderDetail] = orderDetailItr.toList
          for (orderDetail <- orderDetailList ) {
            MyKafkaSink.send("DW_ORDER_DETAIL",orderDetail.order_id.toString,JSON.toJSONString(orderDetail,new SerializeConfig(true)))
          }
        }

      OffsetManager.saveOffset(groupId, topic, offsetRanges)

    }
    ssc.start()
    ssc.awaitTermination()

  }
}
