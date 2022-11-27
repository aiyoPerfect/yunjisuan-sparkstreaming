# yunjisuan-sparkstreaming
## 云计算小组成员

陆文昊 522022000025

曹帅 502022320001

巩翃宇 522022320041

___

---


# 数据获取

## 准备工作

通过指令创建scrapy爬虫项目，针对目标网站stackoverflow创建爬虫文件

```cmd
scrapy startproject mystackoverflow_spider
scrapy genspider stackoverflow stackoverflow.com
```

通过浏览器network获得按时间顺序展示页面的接口https://stackoverflow.com/questions?tab=Newest&page={page}&pagesize=50，并确定爬虫范围

scrapy项目下有以下几个内容：

1、spiders-用于存放所有的具体爬虫项目

2、items.py-定义数据结构- 爬取的数据都包含哪些

3、middlewares.py--中间件 spider和控制器 或者是download和控制器

4、pipelines.py用于爬取数据之后对数据进行处理和存储。

5、settings.py 设置

## 版本介绍

scrapy 2.7.1

java 1.8

fastjson 1.2.28

## 数据获取总体架构

1.通过scrapy获取stackoverflow中最新提问的问题，将问题名称、links、tags、views通过管道以item形式保存在json文件中，为模拟实时大数据，项目中获取了最新的前16500页的数据。
注意在settings中关闭机器人协议

```python
def parse(self, response):
    for index in range(1, 51):
        self.count += 1
        if self.count % 100 == 0:
            logger.info(self.count)

        sel = response.xpath('//*[@id="questions"]/div[{index}]'.format(index=index))
        item = MystackoverflowSpiderItem()
        item['views'] = "".join(
            sel.xpath('div[1]/div[3]/@title').extract()).split()[0].replace(",", "")
        item['questions'] = sel.xpath('div[2]/h3/a/text()').extract()
        item['links'] = "".join(
            sel.xpath('div[2]/h3/a/@href').extract()).split("/")[2]
        tags=[]
        item['tags'] = sel.xpath('//*[@id="questions"]/div[{index}]'.format(index=index)+'//ul/li[@class="d-inline mr4 js-post-tag-list-item"]/a/text()').extract()
        yield(item)
```

2.将保存的json文件切分，提取出所有的tag并保存在txt文件中。由于数据量大，采用fastjson的流式处理方法，逐行读取json文件，将tags均分到多个txt文件中。
```java
public static void readJson(String jsonPath) throws FileNotFoundException {
    int count = 0, path = 0;

    JSONReader reader = new JSONReader(new FileReader(jsonPath));
    reader.startObject();
      while(reader.hasNext()){
        while (reader.hasNext())
        {
            String key = reader.readString();
            if (key.equals("tags"))
            {
                reader.startArray();
                while (reader.hasNext())
                {
                    String item = reader.readString();
                    if(count > 200){
                        path++;
                        count = 0;
                    }
                     writeToFile("tags/tags" + path + ".txt", item);
                    count++;
                    System.out.println(item);
                }
                reader.endArray();
            }
            else{
                String value = reader.readString();
            }
        }
        reader.endObject();
        try{
            reader.startObject();
        }catch(JSONException jsonException){
            break;
        }
    }
    System.out.print("finished!");
}

3.将保存的tag*文件提交到kafka，用于执行分布式流计算


# 使用spark streaming 实现

## spark准备工作

在云主机中/usr/local配置本地spark环境，运行spark-shell，可以正常使用scala编译程序

在云主机分发配置好的spark

```shell
scp -r /usr/local/spark root@worker1:/usr/local
scp -r /usr/local/spark root@worker2:/usr/local
scp -r /usr/local/spark root@worker3:/usr/local
```

启动集群

 ```shell
 /usr/local/spark/bin/start-master.sh
 /usr/local/spark/bin/start-slaves.sh
 ```

打开spark://master:7077查看WebUI,spark stand

### 版本介绍

spark版本3.0.0

hadoop版本3.2

scala版本2.12

kafka版本0.10

##  spark streaming总体架构

先通过producer将数据传入kafka

先创建SparkConf运行配置对象，创建Spark上下文环境对象，主机地址spark://master:7077

```scala
    val sparkConf = new SparkConf().setMaster("spark://master:7077").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
```

定义kafka参数，从worker1产生的Kafka数据读取，创建Dstream，同时将每条消息的KV取出。当前版本不再采用08版本Kafka的Reciver模式，在本项目中010版本的kafka采用Direct模式，使用createDirectStream方法。最红通过start()和awaitAnyTermination()启动和关闭SparkStreamingContext。

```scala
  val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "worker1:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "cloudcompute",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
 val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("wordcount"), kafkaPara)
    )
    val valueDStream: DStream[String] = kafkaDataDS.map(record => record.value())


    ...


    ssc.start()
    ssc.awaitTermination()
```

通过Kafka读入的tag.txt，将每一行数据读入，根据空格切分成单词，随后转变数据结构为(tag,次数)，并根据相同的tag整合。由于DStream没有体统直接排序的方法，需要直接对底层RDD进行操作，通过sortBy算子根据第二个元素的降序进行排列，输出其中使用次数最多的10个tags

```scala
val wordadd = valueDStream.flatMap(_.split(" "))
  .map((_, 1))
  .reduceByKey(_+_)
wordadd.print()
val sortedResultDS: Unit = wordadd.foreachRDD(rdd => {
  val sortRDD: RDD[(String, Int)] = rdd.sortBy(t=>t._2,ascending = false)
  val top10: Array[(String, Int)] = sortRDD.take(10)
  print("=======top10=======\n")
  top10.foreach(println)
  print("=======top10=======\n")
})
```

# 使用flink实现

## flink 准备工作

在前期根据flink手册准备好环境之后，可以通过命令快速启动，集群：

在worker1、worker2、worker3上运行 命令 启动worker 节点的zookeeper和journalnode

```shell
/usr/local/zookeeper/bin/zkServer.sh start
/usr/local/hadoop/sbin/hadoop-daemon.sh start journalnode
```

然后在master上 start-all.sh 启动hadoop集群 start-cluster.sh启动flink集群。

```shell
/usr/local/hadoop/sbin/start-all.sh
/usr/local/flink/bin/start-cluster.sh
```

打开链接`master:8081` 查看web端的flink

## 版本介绍

flink版本1.12

hadoop版本3.2

scala版本2.13

kafka版本3.0.0

## flink 总体构架

1、创建flink运行环境，设置并行度为1

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(4);
```

2、创建flink的数据源，指定连接kafka数据源，创建一个flink-kafka的消费者来消费数据。

```java
Properties properties = new Properties();
//set bootstrap
properties.put("bootstrap.servers","worker1:9092");
//set serialization 反序列化
properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
 //create a consumer
DataStreamSource<String> kafkaDatastream = env.addSource(new FlinkKafkaConsumer<String>("wordcount", new SimpleStringSchema(), properties));

```

3、进行flink中transform算子的运算过程。

首先通过flatmap算子，将读入的文本信息流，转化为单个的Tag类，Tag类的结构如下：

```java
public class Tag {
    public String tag;
    public Long timestamp;
    }
```

其中有一个tag保存读取的目标的标签，有一个timestamp保存当前标签的产生时间。flatmap后数据流变为Tag类型，再通过withTimestampandwatermark方法，为每一个tag赋予一个事件时间，在后续的处理中，根据此事件时间进行开窗统计，这里选择的窗口为间隔5秒持续时间20秒的滑动窗口。

```java
SingleOutputStreamOperator<TagCount> tagCountStream = wordStreamwithStamp.keyBy(data -> data.tag)
        .window(SlidingEventTimeWindows.of(Time.seconds(20), Time.seconds(5)))
        .aggregate(new WordCountAgg.WordCountagg(), new WordCountProcessWinFunc.WordCountResult());
```

flink中核心实现TopN的核心代码为如下的功能，通过重写最底层的processfunction，可以分布式的在各个节点上完成sort的排序后进行合并，最后经过包装打印输出排名前十的 标签。

```java
 public static class TopNProcessResult extends KeyedProcessFunction<Long,TagCount,String>{

        private Integer n;
        public TopNProcessResult(Integer n){
            this.n = n;
        }
        private ListState<TagCount> tagCountListState;

        //get status from environment
        @Override
        public void open(Configuration parameters) throws Exception {
            tagCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<TagCount>("tag-count-list", Types.POJO(TagCount.class))
            );
        }

        @Override
        public void processElement(TagCount value, Context ctx, Collector<String> out) throws Exception {
            //save state
            tagCountListState.add(value);
            //register a timer
            ctx.timerService().registerEventTimeTimer((ctx.getCurrentKey()+1));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<TagCount> tagCountArrayList = new ArrayList<TagCount>();
            for(TagCount tagCount : tagCountListState.get()){
                tagCountArrayList.add(tagCount);
            }

            tagCountArrayList.sort(new Comparator<TagCount>() {
                @Override
                public int compare(TagCount o1, TagCount o2) {
                    return o2.count.intValue()-o1.count.intValue();
                }
            });
            // 包装信息，打印输出
            StringBuilder result = new StringBuilder();
            result.append("---------------------\n");
            result.append(" 窗口结束时间： " + new Timestamp(ctx.getCurrentKey())+"\n");
            // 取list前10个，包装信息输出
            for(int i = 0;i <10;i++){
                TagCount currTuple = tagCountArrayList.get(i);
                String info ="No. " + (i+1) +" "
                        +"tag: " + currTuple.tag + " "
                        +"出现数目：" + currTuple.count + "\n";
                result.append(info);
            }
            result.append("------------------\n");
            out.collect(result.toString());
        }
    }
```

4、通过重写sink方法，可以每次生成一个输出流，将采集到的前十信息写入json文件中，sinkfunction的重写结构如下：

```java
public static class MyRichSinktoJsonWriter extends RichSinkFunction<ArrayList<TagCount>> implements SinkFunction<ArrayList<TagCount>>{
    private static String path;

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            super.open(parameters);
            path = "/root/Documents/json1.json";
            File file = new File(path);
            if(!file.exists()) {
                file.createNewFile();
            }
        }catch (Exception e){
            System.out.println("lod file error");
        }
    }

    @Override
    public void invoke(ArrayList<TagCount> value, Context context) throws Exception {
        JSONArray ja = new JSONArray();
        for(TagCount tag : value){
            JSONObject jo = new JSONObject();
            jo.put("name",tag.tag);
            jo.put("count",tag.count);
            ja.put(jo);
        }
        File file = new File(path);
        FileUtils.write(file,ja.toString(),"utf-8",false);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
```

