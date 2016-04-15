# 用GO语言实现一个简单的搜索引擎


## 注意：：：：：项目完全重构，代码基本完成，还未测试！！！！暂时谨慎直接使用！！README稍后完成全部更新

对搜索引擎感兴趣的可以去看看[这本书](http://www.amazon.cn/%E8%BF%99%E5%B0%B1%E6%98%AF%E6%90%9C%E7%B4%A2%E5%BC%95%E6%93%8E-%E6%A0%B8%E5%BF%83%E6%8A%80%E6%9C%AF%E8%AF%A6%E8%A7%A3-%E5%BC%A0%E4%BF%8A%E6%9E%97/dp/B006J9MSD8)，比较浅并且也比较完整的介绍了一个搜索引擎的全部机能。

## 主要功能点，更新列表
- 类似于数据库的表一样按字段进行存储
- 支持倒排索引字段，正排索引字段，仅仅储存不进行检索的字段
- 倒排索引支持
  - 完全匹配的字符串（类似ID,ISBN等需要完全匹配的属性）
  - 分词类型 （全文索引）
  - 根据特殊标志符进行切分的模式
- 正排索引支持
  - 数字索引（暂时只支持整数，需要自己将其他数转化成整数）
  - 日期索引（目前支持`2005-01-02 00:02:03`和`2005-01-02`两种格式）
  - 不进行检索字段，只存储详细信息
- 实时搜索引擎，索引器和检索器就是同一个服务，通过json方式push数据进引擎，引擎自行就行存储，不需要先进行全量索引建立
- 支持搜索，过滤，汇总，统计四种查询
- 策略引擎部分可以自己实现接口进行扩展
- 无配置文件，只需要在启动的时候指定端口
- 使用MMAP方式进行数据存储和读取
- 使用B+树进行字典和key的存储
- 实时索引，随时进行索引更新
- 默认使用文本相关性进行排序
- 性能测试报告稍后提交


## TODO列表
- 增加选项，可以一次性将所有数据加载进内存
- 索引分片
- 分布式部署，保存多个副本
- 集群化搜索引擎

## 性能测试
- 进行中

## 使用方法
### 依赖以下几个库
- [github.com/apsdehal/go-logger](https://github.com/apsdehal/go-logger) log输出类
- [github.com/go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) MySql类，用于和MySql交互
- 【分词器不依赖了，有一个简单函数进行分词，可以自己加上】[github.com/huichen/sego](https://github.com/huichen/sego) 分词器，作者[主页](https://github.com/huichen)非常感谢他的分词器。




-----




### 编译
- 直接运行`install.sh`
- 在`bin`目录生成`FalconEngine  `可执行文件


------



### 运行
- 新建`data`目录，从[这里](https://github.com/huichen/sego/blob/master/data/dictionary.txt)获取分词的字典文件`dictionary.txt`,存入当前目录的`data`下
- 新建`index`文件夹
- 运行
> bin/FalconEngine  【默认端口9990,用 -p=XXX 修改端口】



--------



### 导入数据

导入数据有三种方式

#### 直接请求导入数据

##### 先建立索引结构
- URL: http://127.0.0.1:9990/v1/_create?index=weibo
- METHOD : **POST**
- BODY
```
{
    "indexmapping":[
    {
		"fieldname":"datetime",  //字段名称
        "fieldtype":15			 //字段类型
	},     
	{
		"fieldname":"name",
        "fieldtype":2
	},
	{
		"fieldname":"level",
        "fieldtype":1
	},
	{
		"fieldname":"content",
        "fieldtype":2
	}
        ]
}
```

##### 添加数据
- URL: http://127.0.0.1:9990/v1/_update?index=weibo
- METHOD : **POST**
- BODY
```
{
    "datetime":"2015-11-12 23:58:22",
    "name":"延参法师",	
    "level":"黄V",
    "content":"看山东，赞山东，和大家一起拉呱，说人生哦。"
}
```

#### 从文件导入

##### 从文件导入的话，先要建立索引，方法同上
##### 从文件导入
可以把文件存成一行一行的json格式，或者用分隔符分割数据
- 分隔符分割的文件类似如下
```
2015-11-12 23:58:22	夢想家林志穎	黄V	加油!!!
2015-11-12 23:58:22	延参法师	黄V	看山东，赞山东，和大家一起拉呱，说人生哦。
2015-11-12 23:58:22	延参法师	黄V	嗯，看时间，潍坊见哦。
2015-11-12 23:58:22	尐笨蛋晴空路口	普通用户	转发微博
```
然后发送请求：
- URL：http://127.0.0.1:9990/v1/_load?index=weibo
- METHOD: **POST**
- BODY:
```json
{
    
    "_split":"\t",  //分隔符
    "_fields":["datetime","name","level","content"], //分隔符对应的字段
    "_filename":"./weibo.log", //文件位置
    "_synccount":200000, 		//多少文档刷新一次磁盘
    "_ismerge":true      		//导入结束后是否将索引合并为一个
}
```

#### 直接从数据库表导入

直接发送如下格式的POST请求到服务器即可

- URL：http://127.0.0.1:9990/v1/_load?index=test_table&fromdb=1
- METHOD: **POST**
- BODY:

```json

{
	"sql" : "SELECT user_id,title,author,content,last_modify_time FROM test_table WHERE is_delete=0",  				  //执行的SQL语句，用来获取全量数据
	"user" : "wyh",  				//数据库用户名
	"password" : "wyh", 			//数据库密码
	"host" : "10.254.33.33", 		//数据库地址
	"port" : "3306",				//数据库端口
	"dbname" : "test_DB",			//数据库名称			
	"charset" : "utf8",				//数据库编码
	"tablename" : "test_table",	//需要同步的表名，注意和SQL要一致
	"indexname" : "test_table",	//表对应的搜索引擎索引的名称
	"mapping" : [					//表和索引字段的对应关系
		{
			"field_db":"user_id",
			"field_index" : "user_id",
			"field_type" : 21		//字段类型
			},
		{
			"field_db":"title",
			"field_index" : "title",
			"field_type" : 2
			},
		{
			"field_db":"content",
			"field_index" : "content",
			"field_type" : 2
			},
		{
			"field_db":"author",
			"field_index" : "author",
			"field_type" : 2
			},
		{
			"field_db":"content",
			"field_index" : "content",
			"field_type" : 30
			},
		{
			"field_db":"last_modify_time",
			"field_index" : "last_modify_time",
			"field_type" : 15
			}
	]
}

```

-----

### 查询

目前支持GET请求的查询，API如下：



| 参数     | 含义    | 备注                             |
| ------ | ----- | ------------------------------ |
| index  | 索引名称  | 必须字段                           |
| q      | 查询关键词 | q=测试，没有该字段，返回所有结果集             |
| ps     | 每页数量  | ps=10  默认为10                   |
| pg     | 返回第几页 | pg=1    默认为1                   |
| show   | 展示字段  | show=content,name,level 默认全部字段 |
| gather | 汇总字段  | gather=level 可选，没填就不汇总         |
| sort   | 是否排序  | sort=false  可选，默认为true，按相关性排序  |



另外，数字和日期类型的字段可以进行过滤操作，比如有两个字段`年龄(age)`和`生日（brith）`，分别是数字和日期字段，那么可以进行如下过滤操作

| 操作符号 | 操作类型 | 备注                                      |
| ---- | ---- | --------------------------------------- |
| '-'  | 等于   | -age=18&-brith=1990-01-02               |
| '_'  | 不等于  | \_age=18&\_brith=1990-01-02             |
| '>'  | 大于   | >age=18&>brith=1990-01-02               |
| '<'  | 小于   | <age=18&<brith=1990-01-02               |
| '~'  | 范围   | ~age=16,24&~brith=1990-01-02,2003-03-04 |
|      |      |                                         |



比如上文中的微博的信息

我们要找发布时间在2013-08-18到2015-12-25发布的微博，并且带有关键字`雅礼中学`的微博，并且还想看看各个级别的人发布了多少条，那么请求就是：



**http://127.0.0.1:9990/v1/_search?index=weibo&q=雅礼中学&ps=10&pg=1&show=name,level,datetime,content&~datetime=2013-08-18,2015-12-25&gather=level**

返回值为：

```json

{
    totalCount: 405,
    from: 1,
    to: 10,
    status: "OK",
    costTime: "3.743808ms",
    Gaters: {
          level: {
          付费会员: 11,
          普通用户: 67,
          蓝V: 84,
          达人: 14,
          黄V: 229
          }
	},
dataDetail: [
        {
        content: "可以回雅礼试试",
        datetime: "2015-11-12 20:26:27",
        level: "普通用户",
        name: "拐栋六R"
        },
        {
        content: "雅礼中学开始报名了",
        datetime: "2015-11-12 18:58:03",
        level: "蓝V",
        name: "爱尔威智能科技"
        }
  ......
}


```













