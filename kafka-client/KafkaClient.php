<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年8月14日
*/

namespace DQD\kafka;

class KafkaClient {
    
    protected static $topicName = '';
    protected static $objProducer = [];
    protected static $objTopic = [];
    
    protected static $conf = [];
    protected static $topicConf = [];
    
    protected static $isRegisterSf = false;
    
    protected static $isSendMsgSync = true;
    protected static $isSendMsgSuccess = true;
    
    protected static $brokers = NULL;
    protected static $logpath = NULL;
    
    public static function init($brokers, $logpath = null)
    {
        self::$brokers = $brokers;
        self::$logpath = $logpath;
    }
    
    public static function setConf($conf)
    {
        if(!is_array($conf)) return false;
        self::$conf = $conf;
    }
    
    public static function setTopicConf($topicConf)
    {
        if(!is_array($topicConf)) return false;
        self::$topicConf = $topicConf;
    }
    
    /**
     * 设置topic名
     * @param unknown $topicName
     */
    public static function setTopic($topicName)
    {
        self::$topicName = $topicName;
    }
    
    public static function  deliveryCallBack($kafka,  $message)
    { 
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;
            default:
                self::$isSendMsgSuccess = false;
                $msg = sprintf("delivery error: %s:%s, %s",$message->err, rd_kafka_err2str($message->err),json_encode($message));
                self::callBackLog($msg,'error');
        }
    }
    
    public static function errorCallBack($kafka, $err, $reason)
    {
        switch ($err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;
            default:
                $msg = sprintf("producer error: %s-%s",$err, $reason);
                self::callBackLog($msg,'error');
        }
    }
    
    public static function  initCallBack($objConf)
    {
        $objConf->setDrMsgCb(array('\DQD\kafka\KafkaClient','deliveryCallBack'));
        $objConf->setErrorCb(array('\DQD\kafka\KafkaClient','errorCallBack'));
    }
    
    public static function getObjectKey()
    {
        $c = empty(self::$conf) ? 'e' : md5(json_encode(self::$conf));
        $ct = empty(self::$topicConf) ? 'e' : md5(json_encode(self::$topicConf));
        return $c.'_'.$ct.'_'.self::$topicName;
    }
    
    /**
     *  异步发送消息，放入本地队列后即返回，耗时极少
     * @param unknown $msg
     * @param unknown $partitionKey
     * @return boolean
     */
    public static function sendMsgAsyn($msg, $partitionKey = NULL,  $cluster =  NULL)
    {
        self::$isSendMsgSync = false;
        return self::realSendMsg($msg, $partitionKey, $cluster);
    }
    
    /**
     *  同步发送消息，消息发送成功再返回，有一定耗时
     * @param unknown $msg
     * @param unknown $partitionKey
     * @return boolean
     */
    public static function sendMsg($msg, $partitionKey = NULL, $cluster =  NULL)
    {
        self::$isSendMsgSync = true;
        return self::realSendMsg($msg, $partitionKey, $cluster);
    }
    
    public static function realSendMsg($msg, $partitionKey = NULL,  $cluster =  NULL)
    {
        if(empty(self::$topicName)) return false;
        if(empty($msg)) return false;
        
        try {
            self::$isSendMsgSuccess = true;
            //conf配置
            //pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
            $objConf = new \RdKafka\Conf();
            self::$conf = array_merge(KafkaConfig::$produceConf, self::$conf);
            
            foreach (self::$conf as $key => $val) {
                $objConf->set($key, $val);
            }
            self::initCallBack($objConf);
            
            //topic conf配置
            $objTopicConf = new \RdKafka\TopicConf();
            self::$topicConf = array_merge(KafkaConfig::$produceTopicConf, self::$topicConf);
            foreach (self::$topicConf as $key => $val) {
                $objTopicConf->set($key, $val);
            }
            $objKey = self::getObjectKey();
            
            if(isset(self::$objProducer[$objKey])) {
                $producer = self::$objProducer[$objKey];
            } else {
                $producer= new \RdKafka\Producer($objConf);
                self::$objProducer[$objKey] = $producer;
            }
       
            $producer->setLogLevel(LOG_ERR);//通过cb获取日志
            
            $brokers = self::$brokers;
            if(empty($brokers)) {
                return false;
            }
            
            $producer->addBrokers($brokers);
            if(isset(self::$objTopic[$objKey])) {
                $topic = self::$objTopic[$objKey];
            } else {
                $topic= $producer->newTopic(self::$topicName, $objTopicConf);
                self::$objTopic[$objKey] = $topic;
            }
            
            if(is_array($msg)) {
                $msg = json_encode($msg);
            }

            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $msg, $partitionKey);
            if(self::$isSendMsgSync) {
                //对象析构时会自动进行poll
                while($producer->getOutQLen() > 0) {
                     $producer->poll(KafkaConfig::$producerPollTimeOut);
                }
            }
            //注册该shutdown函数目的是在请求结束，对象析构时，执行fastcgi_finish_request ，然后异步执行poll，提高响应速度
            //但注意仍会占用PHP进程直至poll完成
            if(!self::$isRegisterSf) {
                register_shutdown_function(array('\DQD\kafka\KafkaClient','kafkaShutDown'));
                self::$isRegisterSf = true;
            }
            if(self::$isSendMsgSync){
                return self::$isSendMsgSuccess;
            } 
            return true;
        } catch (\Exception $e) {
            $msg = $e->getMessage();
            return false; 
        }
    }
    
    public static function kafkaShutDown()
    {
        if(function_exists('fastcgi_finish_request')) {
            fastcgi_finish_request();
        }
    }

    public static function callBackLog ($msg, $type = 'notice')
    {
        $path = self::$logpath;
        if(empty($path)) return false;
        $bt = debug_backtrace( DEBUG_BACKTRACE_PROVIDE_OBJECT, 2);
        $msg = sprintf("[%s] [%s] [%s] [%s] [%s:%s] %s", date('Y-m-d H:i:s', time()), memory_get_usage(), $type, posix_getpid(), $bt[0]['file'], $bt[0]['line'], $msg);
        $file = $type . '.log' . '-' . date('Ymd', time());
      
        
        if(!file_exists($path)) {
            mkdir($path,0755);
        }
        file_put_contents($path.'/'.$file, $msg . "\n", FILE_APPEND);
    }
}
