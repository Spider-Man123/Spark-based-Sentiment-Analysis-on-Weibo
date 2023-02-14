package weiboSentiment

import com.huaban.analysis.jieba.{JiebaSegmenter, WordDictionary}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.tools.ant.filters.StripLineComments.Comment

import scala.collection.immutable.List
import java.io.{BufferedReader, File, FileReader, IOException}
import java.nio.file.Path
import java.util
import java.util.{ArrayList, Arrays, Properties}

object sentiment {
  def main(args: Array[String]): Unit = {
    /**
     * 作用1：对于某个热门事件，通过对公告的舆情分析，来判断该公告对于事件情感走向的影响，为以后处理该事件的公告发布提供有用信息。
     * 作用2：对于某个热门事件，通过对地区情感极性的分析，判断具有某类关键词的事件在哪个区域关注度比较高，群众情感比较消极，来为后续在该地区中发布公告频率的提供支持。
     * 作用3：对于某个热门事件，我们通过所有评论中的语句相似度来分析得到水军集合，在后续工作用我们通过水军们的相互转发等来构建一个水军关系网，从而在
     *       之后的舆情分析中，如果遇到该用户发表的评论进行选择性过滤。
     * 1. 首先使用对每个句子进行情感分析然后构建临时表
     * 2. 使用sparkSQL进行情感聚合，计算每个事件的情感极性总和，计算每个事件的每个公告的情感极性总和
     * 3. 计算每个事件各个地区对其的情感倾向排行
     * 4. 计算每个事件中评论的男女情感比例，分析事件中男女对该事件的不同看法
     * 5. 对所有评论的评价者进行水军判断
     * 步骤：
     *      1. 读取词典对每句话的情感进行计算，并且得到临时表，其中包含 join后得到
     *             事件id 公告id 句子id 句子内容 发布时间 发布者id 来自的地区  性别
     *          计算后增加情感列
     *      2. 使用sparkSQL进行聚合，首先聚合公告id的情感，得到 公告id 情感
     *      3. 聚合事件id情感 得到 事件id 情感
     *      4. 聚合事件对应的各个区域的情感极性，并且得到排行
     *      5. 聚合事件男女分别平均各个情感极性（积极，消极，中性）的分数
     */

    val conf = new SparkConf().setMaster("local[*]").setAppName("weibo_sentiment")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val props = new Properties()
    props.put("user","root")
    props.put("password","258963")

    /*
        表的读取
     */
    val announceContent: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark_test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "258963")
      .option("dbtable", "announce")
      .load()
    val commentContent: DataFrame = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark_test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "258963")
      .option("dbtable", "comments_one")
      .load()
    // join 操作获取全部所需要字段
    println("*********************join数据**********************")
    announceContent.createOrReplaceTempView("announce")
    commentContent.createOrReplaceTempView("comments")
    val content: DataFrame = spark.sql("select b.case_id,a.announce_id,comment_id,comment_content,comment_data,comment_user_id,comment_user_location,comment_user_sex from comments a left join announce b on a.announce_id=b.announce_id")
    /*
      优化：使用广播join，将announce广播，然后使用map完成相同功能，避免shuffle
     */

    /*
      nlp系统可以替换该部分，需要将分词等功能在外部进行，词典加载到nlp系统中
      将得到的结果使用spark处理得到情感分析等需要的内容
    */

    //读取领域情感词典并且加载分词加载为广播变量
    println("*********************加载词典**********************")
    val file: File = new File("data/all_word.txt")
    val path = file.toPath
    WordDictionary.getInstance.loadUserDict(path)
    val pun0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(util.Arrays.asList("?", "!"))
    val zhuan0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(util.Arrays.asList("但是", "但", "然而", "可是"))
    val not_word0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(util.Arrays.asList("不是", "不"))
    val dict_pos0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(get_dicts("积极词"))
    val dict_neg0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(get_dicts("消极词"))
    val first_adv0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(get_dicts("first_adv"))
    val second_adv0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(get_dicts("second_adv"))
    val third_adv0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(get_dicts("third_adv"))
    val forth_adv0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(get_dicts("forth_adv"))
    val fifth_adv0: Broadcast[util.List[String]] = spark.sparkContext.broadcast(get_dicts("fifth_adv"))

    println("*********************情感分析**********************")
    // 对句子进行情感分析
    val data = content.rdd
    data.cache()
    val rdd0: RDD[Row] =data.map(
      row => {
        val sentence = row.get(3)
        val pun = pun0.value
        val zhuan = zhuan0.value
        val not_word = not_word0.value
        val dict_pos = dict_pos0.value
        val dict_neg = dict_neg0.value
        val first_adv = first_adv0.value
        val second_adv = second_adv0.value
        val third_adv = third_adv0.value
        val forth_adv = forth_adv0.value
        val fifth_adv = fifth_adv0.value
        val comb = Array[Double](0.0, 1.0, 1.0, 1.0, 1.0)
        val seg = new JiebaSegmenter
        // 这块也可以在前期工作就进行
        val wordss = seg.sentenceProcess(sentence.toString)
        import scala.collection.JavaConversions._
        for (word <- wordss) {
          if (dict_pos.contains(word)) comb(0) = 1.0
          if (dict_neg.contains(word)) comb(0) = -1.0
          if (pun.contains(word) && comb(0) != 0.0) comb(1) = 2.0
          if (zhuan.contains(word) && comb(0) != 0.0) comb(2) = -1.0
          if (not_word.contains(word) && comb(0) == 0.0) comb(3) = -1.0
          if (first_adv.contains(word) && comb(0) == 0.0) comb(4) = 2.0
          if (second_adv.contains(word) && comb(0) == 0.0) comb(4) = 1.8
          if (third_adv.contains(word) && comb(0) == 0.0) comb(4) = 1.5
          if (forth_adv.contains(word) && comb(0) == 0.0) comb(4) = 0.8
          if (fifth_adv.contains(word) && comb(0) == 0.0) comb(4) = 0.5
        }
        var k = .0
        k = comb(0) * comb(1) * comb(2) * comb(3) * comb(4)
        // 处理发布者位置
        val location = row.get(6).toString.split(" ")(0)
        Row(row.get(0).toString,row.get(1).toString,row.get(2).toString,row.get(3).toString,row.get(4).toString,
         row.get(5).toString,location,row.get(7).toString,k)
      }
    )
    // 创建一个 包含 sentiment 表
    val schema=StructType(
      List(
        StructField("caseId",StringType) ,
        StructField("announceId",StringType) ,
        StructField("commentId",StringType) ,
        StructField("commentContent",StringType) ,
        StructField("date",StringType) ,
        StructField("user",StringType) ,
        StructField("userLocation",StringType) ,
        StructField("userSex",StringType) ,
        StructField("sentiment",DoubleType)
      )
    )
    spark.createDataFrame(rdd0, schema).createTempView("table")
    // 将每个句子id和情感进行存储
    spark.sql("select commentId,sentiment from table").write.jdbc("jdbc:mysql://localhost:3306/spark_test","commentSen",props)
    // 聚合每个事件的情感总分数，积极情感句子数，消极情感句子数，中性情感句子数


    // 聚合每个announce的总情感分数 积极情感句子数 消极情感句子数 中性情感句子数

    spark.sql("select announceId,sum(sentiment) from  table group by announceId").write.jdbc("jdbc:mysql://localhost:3306/spark_test","announceSen",props)
    val announceNeg = spark.sql(
      """
        |select announceId ,count(*)as num from
        |(
        |select announceId,sentiment from table where sentiment<0
        |)t1
        |group by announceId;
        |""".stripMargin)
    val announcePos = spark.sql(
      """
        |select announceId ,count(*)as num from
        |(
        |select announceId,sentiment from table where sentiment>0
        |)t1
        |group by announceId;
        |""".stripMargin)
    val announceMid = spark.sql(
      """
        |select announceId ,count(*)as num from
        |(
        |select announceId,sentiment from table where sentiment=0
        |)t1
        |group by announceId;
        |""".stripMargin)

    // 某件事各个区域对其的关注度
    spark.sql(
      """
        |select caseId,userLocation,count(*) as commentCount from table group by
        |caseId,userLocation order by commentCount desc limit 20
        |""".stripMargin).write.jdbc("jdbc:mysql://localhost:3306/spark_test","caseLocation",props)
    // 某个事件各个区域对其的情感极性句子个数
    // 首先求出某个事件各个区域的积极情感个数，消极情感个数，中性情感个数
    // 然后将三个表进行join得到 caseId location pos neg mid
    spark.sql(
      """
        |select t3.caseId,t3.userLocation,mid,neg,pos from(
        |	select t1.caseId,t1.userLocation,mid,pos from
        |	(
        |		select caseId,userLocation,count(*)as mid from table where sentiment=0
        |		group by caseId,userLocation
        |		)t1
        |	join
        |		(select caseId,userLocation,count(*)as pos from table where sentiment>0
        |		group by caseId,userLocation)t2
        |	on t1.caseId=t2.caseId and t1.userLocation=t2.userLocation
        |	)t3
        |join
        |	(select caseId,userLocation,count(*)as neg from table where sentiment<0
        |	group by caseId,userLocation)t4
        |on t3.caseId=t4.caseId and t3.userLocation=t4.userLocation
        |""".stripMargin).write.jdbc("jdbc:mysql://localhost:3306/spark_test","caseLocationSen",props)

    // 某个事件中男女对其的情感倾向 caseId userSex pos mid neg
    spark.sql(
      """
        |select t3.caseId,t3.userSex,pos,mid,neg from
        |(select t1.caseId,t1.userSex,pos,mid from
        |(select caseId,userSex,count(*) pos from table where sentiment>0 group by caseId,userSex)t1
        |join
        |(select caseId,userSex,count(*) mid from table where sentiment=0 group by caseId,userSex)t2
        |on t1.caseId=t2.caseId and t1.userSex=t2.userSex
        |)t3
        |join
        |(select caseId,userSex,count(*) neg from table where sentiment<0 group by caseId,userSex)t4
        |on t3.caseId=t4.caseId and t3.userSex=t4.userSex
        |""".stripMargin).write.jdbc("jdbc:mysql://localhost:3306/spark_test","caseSex",props)

    // 判断水军
    // 根据评论内容和评论者进行判别
    spark.sql(
      """
        |select user,commentContent,count(*)
        |from table
        |group by  user,commentContent
        |having count(*)>=2;
        |""".stripMargin).write.jdbc("jdbc:mysql://localhost:3306/spark_test","navy",props)



    spark.close()


  }

  @throws[IOException]
  def get_dicts(name: String): util.List[String] = {
    val dict = new util.ArrayList[String]
    val fr = new FileReader(String.format("data/%s.txt", name))
    val br = new BufferedReader(fr)
    var k = br.readLine()
    while (k!=null)
      {dict.add(k)
       k=br.readLine()
      }
    br.close()
    fr.close()
    dict
  }
}
