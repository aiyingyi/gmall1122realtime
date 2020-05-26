package com.atguigu.gmall1122.realtime.app.dws

import java.{lang, util}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall1122.realtime.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.atguigu.gmall1122.realtime.util.{MyKafkaSink, MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object OrderDetailWide1122App {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dws_order_wide_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // ORDER_INFO 数据
    val topicOrderInfo = "DW_ORDER_INFO";
    // ORDER_DETAIL数据
    val topicOrderDetail = "DW_ORDER_DETAIL";
    val groupId = "dws_order_wide_group"


    /////////////////////  偏移量处理///////////////////////////
    // 获取两个流在kafka中的偏移量
    var offsetOrderInfo: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topicOrderInfo)
    var offsetOrderDetail: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topicOrderDetail)


    var inputOrderInfoDstream: InputDStream[ConsumerRecord[String, String]] = null
    var inputOrderDetailDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    //加载orderInfo流
    if (offsetOrderInfo != null && offsetOrderInfo.size > 0) {
      inputOrderInfoDstream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, offsetOrderInfo, groupId)
    } else {
      inputOrderInfoDstream = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, groupId)
    }
    //加载orderDetail流
    if (offsetOrderInfo != null && offsetOrderInfo.size > 0) {
      inputOrderDetailDstream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, offsetOrderDetail, groupId)
    } else {
      inputOrderDetailDstream = MyKafkaUtil.getKafkaStream(topicOrderDetail, ssc, groupId)
    }


    //取得偏移量步长
    var orderInfoOffsetRanges: Array[OffsetRange] = null
    var orderDetailOffsetRanges: Array[OffsetRange] = null

    val inputOrderInfoGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputOrderInfoDstream.transform { rdd =>
      orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }
    val inputOrderDetailGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputOrderDetailDstream.transform { rdd =>
      orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    // 将流中的数据转换成JSON对象
    val orderInfoDstream: DStream[(Long,OrderInfo)] = inputOrderInfoGetOffsetDstream.map { record =>
      val orderJsonstr: String = record.value()
      val orderInfo: OrderInfo = JSON.parseObject(orderJsonstr, classOf[OrderInfo])
      ( orderInfo.id,orderInfo)
    }

    val orderDetailDstream: DStream[(Long,OrderDetail)] = inputOrderDetailGetOffsetDstream.map { record =>
      val orderDetailJsonstr: String = record.value()
      val orderDetail: OrderDetail = JSON.parseObject(orderDetailJsonstr, classOf[OrderDetail])
      ( orderDetail.order_id,orderDetail  )
    }

    // 对两个流进行开窗
    //窗口开小了 数据延迟大的话 还是会出现丢失 //窗口开大了 会造成大量冗余数据
    val orderInfoWindowDstream: DStream[(Long, OrderInfo)] = orderInfoDstream.window(Seconds(15),Seconds(5))
    val orderDetailWindowDstream: DStream[(Long, OrderDetail)] = orderDetailDstream.window(Seconds(15),Seconds(5))

    // 会不会出现shuffle  // 让相同订单的明细保持在一个分区下-> 写入kafka时 ，选用order_id 作为key
    val orderJoinedDstream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoWindowDstream.join(orderDetailWindowDstream)

    val orderDetailWideDstream: DStream[OrderDetailWide] = orderJoinedDstream.map{case (orderId,(orderInfo,orderDetail))=>new OrderDetailWide(orderInfo,orderDetail)}

    //去重
    val orderDetailWideFilteredDstream: DStream[OrderDetailWide] = orderDetailWideDstream.mapPartitions { orderWideItr =>
      val jedis: Jedis = RedisUtil.getJedisClient
      val orderWideList: List[OrderDetailWide] = orderWideItr.toList
      // 新建空的集合
      val orderWideFilteredList: ListBuffer[OrderDetailWide] = ListBuffer[OrderDetailWide]()
      for (orderWide <- orderWideList) {
        // redis    type  sadd   key  order_wide:order_id:[order_id]  value  [order_detail_id]  expire 600
        // 如何判断重复？join之后，生成流的数据类型为[(Long, (OrderInfo, OrderDetail))]，Long表示订单号
        // 一个订单对应多个订单明细，在redis中，key使用订单号表示唯一，value类型为set，存放明细id
        val orderWideKey = "order_wide:order_id:" + orderWide.order_id
        // 判断明细id放入zset是否成功
        val isNew: lang.Long = jedis.sadd(orderWideKey, orderWide.order_detail_id.toString)
        jedis.expire(orderWideKey, 600)
        // 如果没有重复，添加到集合
        // 加入成功，不是重复的数据
        if (isNew == 1L) {
          // 放入到这一次开窗产生的集合里面
          orderWideFilteredList += orderWide
        }
      }
      jedis.close()
      orderWideFilteredList.toIterator
    }



    //  orderWide. final_total_amount 实付总金额
    //             origin_total_amount  应付总金额 =  sum( sku_price* sku_num )  单价* 个数
    //             sku_price 商品单价  sku_num 商品购买个数
    // 目标 ： final_detail_amount  明细分摊实付金额       明细分摊实付金额 / 实付总金额 = (单价* 个数)/ 应付总金额
    //但是还要考虑    必须保证 sum(明细分摊实付金额) = 实付总金额
    //    orderDetailWideFilteredDstream.mapPartitions{orderWideItr=>
    //      orderWideItr.toList
    //      null
    //    }

    // OrderDetailWide封装了一个订单明细和对应的订单
    val orderDetailWideWithSplitDsteam: DStream[OrderDetailWide] = orderDetailWideFilteredDstream.mapPartitions { orderWideItr =>
      // jedis
      val jedis: Jedis = RedisUtil.getJedisClient
      // 首先 从 redis中取得该笔订单的已经存入（计算）的明细数据
      val orderDetailWideList: List[OrderDetailWide] = orderWideItr.toList
      for (orderWide <- orderDetailWideList) {
        // redis    type  ?  list     key ?   order_wide:split:[order_id]   value ? json{ sku_price:xxx ,sku_num:xxx,final_detail_amount:xxx}   expire  600
        var orderWideSplitKey = "order_wide:split:" + orderWide.order_id
        val orderWideSplitList: util.List[String] = jedis.lrange(orderWideSplitKey, 0, -1)

        var originAmountSum = 0D
        var finalAmountSum = 0D
        //  把兄弟明细的应收金额+自己的应收金额 得到一个汇总值
        //  把兄弟明细中的实收分摊汇总值 求出
        import scala.collection.JavaConversions._
        if (orderWideSplitList != null && orderWideSplitList.size() > 0) {
          for (splitJson <- orderWideSplitList) {
            val splitJsonObj: JSONObject = JSON.parseObject(splitJson)
            // 计算同一订单的所有订单明细的应收金额总值
            originAmountSum += splitJsonObj.getDouble("sku_price") * splitJsonObj.getDouble("sku_num")

            finalAmountSum += splitJsonObj.getDouble("final_detail_amount")
          }
        }
        //比较  主订单的应收总值是否= 明细的应收汇总值(含自己）
        // 如果等于，则表示此时的这笔明细就是最后一笔明细
        if (orderWide.original_total_amount == originAmountSum + orderWide.sku_price * orderWide.sku_num) {
          // 用减法 总实付-兄弟明细的分摊汇总
          orderWide.final_detail_amount = Math.round((orderWide.final_total_amount - finalAmountSum) * 100) / 100D
        } else {
          //如果不等于 （非最后一笔）
          // 用乘除占比 求得 该明细分摊金额  公式： 明细分摊实付金额？ / 实付总金额 = (单价* 个数)/ 应付总金额
          //明细分摊实付金额？= 实付总金额*(单价* 个数) / 应付总金额
          orderWide.final_detail_amount = Math.round(orderWide.final_total_amount * (orderWide.sku_price * orderWide.sku_num) / orderWide.original_total_amount * 100) / 100D
        }

        //把当前明细的计算结果保存到redis 中
        val curObject = new JSONObject()
        curObject.put("sku_num", orderWide.sku_num)
        curObject.put("sku_price", orderWide.sku_price)
        curObject.put("final_detail_amount", orderWide.final_detail_amount)
        jedis.lpush(orderWideSplitKey, curObject.toJSONString)

      }
      jedis.close()
      orderDetailWideList.toIterator
    }





    orderDetailWideWithSplitDsteam.foreachRDD{rdd=>
      rdd.foreachPartition { orderWideItr =>
        for (orderWide <- orderWideItr) {


          MyKafkaSink.send("DWS_ORDER_DETAIL_WIDE",  JSON.toJSONString(orderWide,new SerializeConfig(true)))
        println(JSON.toJSONString(orderWide,new SerializeConfig(true)))
        }
      }
      OffsetManager.saveOffset(groupId,topicOrderInfo,orderInfoOffsetRanges)
      OffsetManager.saveOffset(groupId,topicOrderDetail,orderDetailOffsetRanges)
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
