package com.atguigu.gmall1122.realtime.app.ads

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1122.realtime.bean.OrderDetailWide
import com.atguigu.gmall1122.realtime.util.{MyKafkaUtil, OffsetManager, OffsetManagerM}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, rdd}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs


object SpuAmountSumApp {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ads_spu_amount_sum_app")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val topic = "DWS_ORDER_DETAIL_WIDE";
    val groupId = "ads_spu_amount_sum_group"


    /////////////////////  偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManagerM.getOffset(groupId, topic)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    val orderDstreamDetailWideDstream: DStream[OrderDetailWide] = inputGetOffsetDstream.map { record =>
      val jsonStr: String = record.value()
      val orderDetailWide: OrderDetailWide = JSON.parseObject(jsonStr, classOf[OrderDetailWide])
      orderDetailWide
    }
    val orderWideWithSpuDstream: DStream[(String, (String,Double))] = orderDstreamDetailWideDstream.map(orderWide=>(orderWide.spu_id+":"+orderWide.spu_name,(orderWide.create_time,  orderWide.final_detail_amount)))

    val spuAmountDstream: DStream[(String, (String,Double))] = orderWideWithSpuDstream.reduceByKey { case ((createTime1, amount1), (createTime2, amount2)) =>
      var earlyCreateTime = if (createTime1<createTime2) createTime1 else createTime2
      (earlyCreateTime, amount1+amount2)
    }
    spuAmountDstream.foreachRDD { rdd =>
      val resultArr: Array[(String,( String, Double))] = rdd.collect()
      if (resultArr != null && resultArr.size > 0) {
        DBs.setup()
        DB.localTx(implicit session => {

          for ((spu, (create_time,amount)) <- resultArr) {
            val spuArr: Array[String] = spu.split(":")
            val spuId: String = spuArr(0)
            val spuName: String = spuArr(1)
            SQL("INSERT INTO spu_order_final_detail_amount_stat(stat_time,spu_id, spu_name, amount) VALUES (?,?,?,?)").bind(create_time, spuId, spuName, amount).update().apply()
          }
          throw new RuntimeException("测试异常！！")
          for (offset <- offsetRanges) {
            //主键相同替换 主键不同新增
            SQL("replace INTO offset_2020(group_id,topic, partition_id, topic_offset) VALUES (?,?,?,?)").bind(groupId, topic, offset.partition, offset.untilOffset).update().apply()
          }

        }

        )
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
