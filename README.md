# kafka-consumer-proxy
kafka消费者代理
## 详细介绍
见 https://blog.csdn.net/MICweaver/article/details/85041252

## 整体架构
![架构图](https://img-blog.csdnimg.cn/20181216214954189.png?x-oss-process=image/watermark,type_ZmFuZ3poZW5naGVpdGk,shadow_10,text_aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L01JQ3dlYXZlcg==,size_16,color_FFFFFF,t_70)
## 示例配置
当一个业务需要处理消息时，首先要准备一个消费消息的http接口，然后提供proxy 的配置文件，最后启动相应消费者进程组即可， proxy会自动转发消息。 proxy配置是一个php文件，如下所示：
 
```
return [
    'host' => '127.0.0.1', 
    'port' => '80', 
    'url' => '/inner/mqdemo/test',
    'concurrency' => 3,
    'can_skip' => false,
    'retry_nums' => 3, 
    'retry_interval_ms' => 2000, 
    'conn_timeout_ms' => 10000, 
    'exec_timeout_ms' => 60000, 
    'domain' => 'api.dqd.com',
    'use_saved_offset_time' => 0,
    'slow_time_ms' => 1000, 
];

```
各配置项的具体说明：

| 配置项 | 示例值  | 是否必选| 说明|
|--|--|--|--|
host	|127.0.0.1|	必选	|ip或域名，多个以‘，’ （逗号）分隔|
port|	80	|必选	|端口|
url	|/inner/mqdemo/test'	|必选	|http请求url|
concurrency|	3	|必选|	消息转发并发数，实际等于启动的消费者进程数|
can_skip|	false	|必选|	消息送达出错时能否跳过，不能跳过会一直重试|
retry_nums|	3|	必选	|消息出错能跳过时重试次数|
retry_interval_ms|	2000	|必选|	出错重试间隔|
conn_timeout_ms	|10000	|必选	|http连接超时|
exec_timeout_ms|	60000|	必选|	http执行超时|
use_saved_offset_time|	0|	必选|	kafka服务在出现offset问题时需要重置offset的时间间隔，一般设为0，出问题立即重置offset|
slow_time_ms|	0|	可选|	慢请求阈值，当转发耗时超过该值则会打印相应消息日志|
domain	|‘api.dqd.com’|可选|	http请求 header host字段|
consumer_conf|[]|	可选|	kafka配置，参见：https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md|
consumer_topic_conf|	['auto.offset.reset' => 'smallest']|	可选	|kafka配置，参见：https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md|
async_concurrency|	3|	可选|	同一个消费进程的处理并发数，不保证消息顺序与消息一定投递成功|
cluster|	‘kafka_dqd’|	可选|	集群配置，当有多个kafka集群时，可以进行切换|
message_delivery_guarantees|	2|	可选|	消息投递保证, 可能值为1、2、3 ，见下文解释|

### 一些特殊配置的解释
#### can_skip：
消息转发出错时能否跳过，不能跳过会一直重试，即一般情况下我们是要保证消息一定成功转发的。 但有时我们可能想手动跳过堵住的命令，有相应脚本工具可以实现。
#### async_concurrency ：
标识转发消息时是同步还是异步，默认是同步，即一个消息必须处理成功后才会处理下一条消息，如果将async_concurrency 值配置为大于0，则会异步并发的转发消息，消息处理速度更快，但不保证每一条消息成功转发，同时也不保证同一个partition中消息的处理顺序
#### message_delivery_guarantees：
kafka每条消息都会有一个编号（即offset)，每成功消费一条消息，proxy会记录相应的offset到kafka集群（broker)，这样当进程重启时，会知道下一条需要消费的消息起点。
为了防止消息丢失，我们只有在一条消息投递成功时才会将其offset记录到服务器， 但进程可能意外退出，已经成功消费的消息offset没有提交成功，
导致进程重启后重复处理该消息，为了防止这种情况，我们将成功处理的消息offset立即写入本地文件，这样重复到来的消息会被我们过滤掉。
当消费者进程多机部署时，一个队列的消息可能先后由不同的机器的进程消费，写入本地的offset信息不能由另一台机器获得，导致消息可能重复，但只会重复处理一条消息。
对于不同业务队列可以采用不同的消息投递保证：
  1. 为了proxy的高可用，我们会多机部署运行proxy, 但在进程意外退出，如人为误杀进程，可能会导致已经成功消费的消息重复消费，但概率极小， 这是我们默认采用的模式， 将 message_delivery_guarantees 的值配置为 2，即消息可能极小概率重复，但不会丢失
2. 在不考虑写本地文件失败的情况下，如果要保证消息的不重复也不丢失，如支付相关的重要数据，我们要单机部署消费者进程，同时将 message_delivery_guarantees 设置为3，即消息不会重复也不会丢失。
3. 每条消息处理完提交offset可能给kafka服务器带来压力，可以间隔一段时间批量提交offset, 将message_delivery_guarantees 的值设为1, 消息可能会丢失，也可能极小概率消息会重复。
