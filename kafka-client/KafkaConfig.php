<?php
/*
 * @desc
 * @author lizhonghua@dongqiudi.com
 * @2017年8月16日
 */

namespace DQD\kafka;
class KafkaConfig {
    
    public static $produceConf = [
        'socket.timeout.ms' => 10000, //网络操作超时，非连接操时，类似执行超时
        'queue.buffering.max.ms' => 0, //低延迟设置
        'message.send.max.retries' => 2,
        'socket.blocking.max.ms' => 50, //过大会导致客户端poll等待时间很长,跟queue.buffering.max.ms 作用一样，Deprecated
        //'internal.termination.signal' => SIGIO,
    ];
    
    public static $produceTopicConf = [
        'message.timeout.ms' => 3000,//delivery成功超时， 太长broker挂掉会hang死php进程  ，类似写超时
        'request.timeout.ms' => 3000, // ack超时，类似读超时
    ];
    
    public static $producerPollTimeOut = 1;
    public static $consumerPollTimeOut = 10000;
    
}
