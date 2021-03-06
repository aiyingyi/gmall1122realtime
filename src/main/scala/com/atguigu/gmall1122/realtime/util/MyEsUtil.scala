package com.atguigu.gmall1122.realtime.util

import java.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder


object MyEsUtil {
  private var factory: JestClientFactory = null;
  // 通过工厂获取JestClient对象
  def getClient: JestClient = {
    if (factory == null) build();
    factory.getObject
  }
  // 通过配置信息，创建JestClientFactory
  def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop105:9200")
      .multiThreaded(true)
      .maxTotalConnection(20)
      .connTimeout(10000).readTimeout(10000).build())
  }


  // batch  批量保存，es的api中，批量保存的操作是Bulk
  def saveBulk(dataList:List[(String,AnyRef)],indexName:String ): Unit ={
    if(dataList!=null&&dataList.size>0){
      val jest: JestClient = getClient
      val bulkBuilder = new Bulk.Builder()
      // 为一个批次的index操作设置默认的index名字和type
      bulkBuilder.defaultIndex(indexName).defaultType("_doc")
      // bulk可以封装多个index，Update等对象
      for ((id,data) <- dataList ) {
        val index: Index = new Index.Builder(data).id(id).build()
        bulkBuilder.addAction(index)
      }
      val bulk: Bulk = bulkBuilder.build()
      val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
      println("已保存："+items.size()+"条数据！")
      jest.close()

    }
  }


  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient
    // Builder模式：
    val index: Index = new Index.Builder(Movie(1,"红海行动",8.5)).index("movie_chn1122").`type`("move").id("1").build()
    jest.execute(index)
    // 查询数据

    // 不使用json字符串来设置查询条件
    // var query:String = "{  \"query\": {\"match\": {   \"name\": \"行动\"  }}}";
    /* 构建查询条件
       通过SearchSourceBuilder最后会将查询条件转换成对应的json字符串，这样就不用在代码里直接写字符串了
       SearchSourceBuilder在es的依赖里面
     */
    val sourceBuilder = new SearchSourceBuilder
    sourceBuilder.query(new MatchQueryBuilder("name","红海行动"))
    sourceBuilder.sort("doubanScore",SortOrder.ASC)
    val query2: String = sourceBuilder.toString

    // 创建 search对象
    val search: Search = new  Search.Builder(query2).addIndex("movie_chn1122").addType("move").build()

    val result: SearchResult = jest.execute(search)

    // 获取命中的document，传入样例类对象，或者Map，对数据进行封装
    val hitList: util.List[SearchResult#Hit[Movie, Void]] = result.getHits(classOf[Movie])

    // 生成的集合是java的。需要导入隐式转换
    import scala.collection.JavaConversions._
    for(hit <- hitList){
      // source 表示获取每条数据的字段部分，不包括索引，type以及document的id。
      val source: Movie = hit.source
      println(source.name)
    }
    // 关闭连接
    jest.close()

  }
}
// 构建样例类
case class Movie(id:Long,name:String,doubanScore:Double)
