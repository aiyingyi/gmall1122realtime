package com.atguigu.gmall1122.realtime.app.dw

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.gmall1122.realtime.bean.{OrderInfo, UserState}
import com.atguigu.gmall1122.realtime.util.{MyEsUtil, MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object OrderInfoApp {


  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dw_order_info_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 数据更改所在表格对应的topic
    val topic = "ODS_T_ORDER_INFO";
    val groupId = "base_order_info_group"


    /////////////////////  偏移量处理///////////////////////////
    val offset: Map[TopicPartition, Long] = OffsetManager.getOffset(groupId, topic)

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    // 判断如果从redis中读取当前最新偏移量 则用该偏移量加载kafka中的数据  否则直接用kafka读出默认最新的数据
    if (offset != null && offset.size > 0) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offset, groupId)
      //startInputDstream.map(_.value).print(1000)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //取得偏移量步长
    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream: DStream[ConsumerRecord[String, String]] = inputDstream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }


    /////////////////////  业务处理///////////////////////////

   //基本转换 补充日期字段
    val orderInfoDstream: DStream[OrderInfo] = inputGetOffsetDstream.map { record =>
      val jsonString: String = record.value()
      // 将JSON数据转换成bean对象
      val orderInfo: OrderInfo = JSON.parseObject(jsonString,classOf[OrderInfo])
      //  日期格式 2020-04-24 19:06:10
      val datetimeArr: Array[String] = orderInfo.create_time.split(" ")
      orderInfo.create_date=datetimeArr(0)
      val timeArr: Array[String] = datetimeArr(1).split(":")
      orderInfo.create_hour=timeArr(0)
      orderInfo
    }

    // 以分区为单位，进行一次查询
    val orderInfoWithfirstDstream: DStream[OrderInfo] = orderInfoDstream.mapPartitions { orderInfoItr =>
      // 将集合中的每个分区的数据转换成一个List
      val orderInfoList: List[OrderInfo] = orderInfoItr.toList
      if(orderInfoList.size>0){
        // 获取所有用户的id列表
          val userIdList: List[String] = orderInfoList.map(_.user_id.toString)
          // sql，查询用户的状态，是否首次消费
          var sql = "select user_id,if_consumed from user_state1122 where user_id in ('" + userIdList.mkString("','") + "')"
          val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)

          // 将根据这个分区的查询的结果，由List转换成Map，这样就相当于构建了一个索引，类似于map join
        // List[JSONObj]=>List[Tumple2]=>Map[String,String]
          val userStateMap: Map[String, String] = userStateList.map(userStateJsonObj =>
            //注意返回字段的大小写！！！！！！！！
            (userStateJsonObj.getString("USER_ID"), userStateJsonObj.getString("IF_CONSUMED"))
          ).toMap
          for (orderInfo <- orderInfoList) {
            // 假如在Map
            val userIfConsumed: String = userStateMap.getOrElse(orderInfo.user_id.toString, null)
            if (userIfConsumed != null && userIfConsumed == "1") {
              orderInfo.if_first_order = "0"
            } else {
              orderInfo.if_first_order = "1"
            }
          }
      }
      orderInfoList.toIterator
    }

    // 解决 同一批次同一用户多次下单  如果是首次消费 ，多笔订单都会被认为是首单
    // 在同一个批次内，一个用户多次下单，那么就会造成多个订单都是首单，并且同一个用户的订单还可能不在同一个分区中
    // 解决方法：分组：同一个用户的订单放在一组，groupByKey算子，会进行shuffle，按照key进行聚合

    // 转换成（user_id,OrderInfo）
    val orderInfoWithUidDstream: DStream[(Long, OrderInfo)] = orderInfoWithfirstDstream.map(orderInfo=>(orderInfo.user_id,orderInfo))
    // 按照key分组聚合
    val orderInfoGroupbyUidDstream: DStream[(Long, Iterable[OrderInfo])] = orderInfoWithUidDstream.groupByKey()
    // 再展开
    val orderInfoFinalFirstDstream: DStream[OrderInfo] = orderInfoGroupbyUidDstream.flatMap { case (userId, orderInfoItr) =>
      {
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
        // 假如这个批次的同一用户的订单，随便有一个是首单标志，那么其他也是首单，
        if (orderInfoList(0).if_first_order == "1" && orderInfoList.size > 1) { //有首单标志的用户订单集合才进行处理
          //把本批次同一用户的订单按照时间排序
          val orderInfoSortedList: List[OrderInfo] = orderInfoList.sortWith { (orderInfo1, orderInfo2) => (orderInfo1.create_time < orderInfo2.create_time) }
          for (i <- 1 to orderInfoSortedList.size - 1) {
            orderInfoSortedList(i).if_first_order = "0" //除了第一笔全部置为0 （非首单)
          }
          orderInfoSortedList.toIterator
        } else {
          orderInfoList.toIterator
        }
    }
    }

//    orderInfoFinalFirstDstream.mapPartitions{
//      //每个分区查一次  12次  100次
//
//    }

    //查询次数过多
//    orderInfoFinalFirstDstream.map{orderInfo=>
//
//
//      val sql="select id ,name , region_id ,area_code  from gmall1122_base_province where id= '"+orderInfo.province_id+"'"
//      val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
//      if(provinceJsonObjList!=null&&provinceJsonObjList.size>0){
//        orderInfo.province_name= provinceJsonObjList(0).getString("NAME")
//        orderInfo.province_area_code= provinceJsonObjList(0).getString("AREA_CODE")
//      }
//      orderInfo
//
//    }
// 考虑到查询的整表数据量很小  可以通过一次查询 再通过广播变量进行分发
//问题：如果数据发生变动 无法感知 因为 算子外面的driver操作 只有启动时会执行一次 之后再不执行了
 /*   val sql="select id ,name , region_id ,area_code  from gmall1122_base_province  "   //driver 只执行一次 启动
    val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
    val provinceListBc: Broadcast[List[JSONObject]] = ssc.sparkContext.broadcast(provinceJsonObjList)
    orderInfoFinalFirstDstream.map{orderInfo:OrderInfo=>
          val provinceJsonObjListFromBC: List[JSONObject] = provinceListBc.value   //executor
          //list转map
          val provinceJsonObjMap: Map[Long, JSONObject] = provinceJsonObjListFromBC.map(jsonObj=>(jsonObj.getLongValue("ID"),jsonObj)).toMap
           val provinceJsonObj: JSONObject = provinceJsonObjMap.getOrElse(orderInfo.province_id,null)//从map中寻值
          if(provinceJsonObj!=null){
            orderInfo.province_name=provinceJsonObj.getString("NAME")
            orderInfo.province_area_code=provinceJsonObj.getString("AREA_CODE")
          }
         orderInfo
    }*/

    // orderInfoFinalFirstDstream本批次所有首单已经做好了标记
    val orderWithProvinceDstream: DStream[OrderInfo] = orderInfoFinalFirstDstream.transform { rdd =>

      // 获取地区信息，转换成列表
      if(rdd.count()>0){
        //driver
        val sql = "select id ,name , region_id ,area_code  from gmall1122_base_province  " //driver  周期性执行
        val provinceJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)

        //list转map
        val provinceJsonObjMap: Map[Long, JSONObject] = provinceJsonObjList.map(jsonObj => (jsonObj.getLongValue("ID"), jsonObj)).toMap
        //广播这个map，广播变量也可以在算子中进行创建
        val provinceJsonObjMapBc: Broadcast[Map[Long, JSONObject]] = ssc.sparkContext.broadcast(provinceJsonObjMap)
        val orderInfoWithProvinceRDD: RDD[OrderInfo] = rdd.mapPartitions { orderInfoItr => //ex
        val provinceJsonObjMap: Map[Long, JSONObject] = provinceJsonObjMapBc.value //接收bc
        val orderInfoList: List[OrderInfo] = orderInfoItr.toList
          for (orderInfo <- orderInfoList) {
            val provinceJsonObj: JSONObject = provinceJsonObjMap.getOrElse(orderInfo.province_id, null) //从map中寻值
            if (provinceJsonObj != null) {
              orderInfo.province_name = provinceJsonObj.getString("NAME")
              orderInfo.province_area_code = provinceJsonObj.getString("AREA_CODE")
            }
          }
          orderInfoList.toIterator
        }
        orderInfoWithProvinceRDD
      }else{
        rdd
      }

    }

    /////////////// 合并 用户信息////////////////////
    val orderInfoWithUserDstream: DStream[OrderInfo] = orderWithProvinceDstream.mapPartitions { orderInfoItr =>
      val orderList: List[OrderInfo] = orderInfoItr.toList
      if(orderList.size>0) {
        val userIdList: List[Long] = orderList.map(_.user_id)
        val sql = "select id ,user_level ,  birthday  , gender  , age_group  , gender_name from gmall1122_user_info where id in ('" + userIdList.mkString("','") + "')"
        val userJsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
        val userJsonObjMap: Map[Long, JSONObject] = userJsonObjList.map(userJsonObj => (userJsonObj.getLongValue("ID"), userJsonObj)).toMap
        for (orderInfo <- orderList) {
          val userJsonObj: JSONObject = userJsonObjMap.getOrElse(orderInfo.user_id, null)
          orderInfo.user_age_group = userJsonObj.getString("AGE_GROUP")
          orderInfo.user_gender = userJsonObj.getString("GENDER_NAME")
        }
      }
      orderList.toIterator
    }



    orderWithProvinceDstream.cache()

    orderWithProvinceDstream.print(1000)

    //写入用户状态，写操作
    orderWithProvinceDstream.foreachRDD { rdd =>
      val userStatRDD: RDD[UserState] = rdd.filter(_.if_first_order == "1").map(orderInfo =>
        UserState(orderInfo.user_id.toString, orderInfo.if_first_order)
      )
      /**
       * 导入spark和phoenix的隐式转换,saveToPhoenix可以将RDD中的数据写入到HBASE的表中
       */
      import org.apache.phoenix.spark._
      userStatRDD.saveToPhoenix("user_state1122",
        Seq("USER_ID", "IF_CONSUMED"),   // 导入表的字段名
        new Configuration,
        Some("hadoop105,hadoop106,hadoop107:2181"))
    }
    //写入es ，开启另外一个job
    //   println("订单数："+ rdd.count())
    orderWithProvinceDstream.foreachRDD{rdd=>
        rdd.foreachPartition{orderInfoItr=>
          val orderList: List[OrderInfo] = orderInfoItr.toList
          val orderWithKeyList: List[(String, OrderInfo)] = orderList.map(orderInfo=>(orderInfo.id.toString,orderInfo))
          val dateStr: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
        //  MyEsUtil.saveBulk(orderWithKeyList,"gmall1122_order_info-"+dateStr)

          for (orderInfo <- orderList ) {
            println(orderInfo)
            MyKafkaSink.send("DW_ORDER_INFO",orderInfo.id.toString,JSON.toJSONString(orderInfo,new SerializeConfig(true)))
          }

        }

      OffsetManager.saveOffset(groupId, topic, offsetRanges)

    }





    ssc.start()
    ssc.awaitTermination()

  }
}
