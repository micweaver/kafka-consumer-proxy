<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年8月14日
*/

namespace DQD\kafka;
require dirname(__FILE__).'/KafkaConfig.php';
require dirname(__FILE__).'/KLog.php';


$__consumerStop = false;
$__skipFailMsg = false;

function sig_handler($signo)
{
    global $__consumerStop, $__skipFailMsg;
    switch ($signo) {
        case SIGTERM:
        case SIGHUP:
        case SIGQUIT:
            $__consumerStop = true;
            break;
        case SIGUSR1:
            $__skipFailMsg = true;
            break;
        default:
            // handle all other signals
    }
}

pcntl_signal(SIGTERM, "DQD\kafka\sig_handler");
pcntl_signal(SIGHUP, "DQD\kafka\sig_handler");
pcntl_signal(SIGQUIT, "DQD\kafka\sig_handler");
pcntl_signal(SIGUSR1, "DQD\kafka\sig_handler");

class ConsumerBase {
    
    protected $topic = NULL;
    protected $group = NULL;
   
    protected $isRebalance = false;
    public function __construct($topic, $group)
    {
        $this->topic = $topic;
        $this->group = $group;
    }
    
    public function errorCallBack($kafka, $err, $reason)
    {
        switch ($err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                break;
            default:
                $msg = sprintf("consumer error:  %s-%s, %s-%s",$err, $reason, $this->topic, $this->group);
                KLog::error($msg);
        }
    }
    
    public function statsCallBack($kafkaConsumer,$res, $len)
    {
        $LogName = sprintf("stats_%s_%s", $this->topic, $this->group);
        //KLog::monitor($res,$LogName,true);
    }

    public function rebalanceCallBack($kafka, $err,  $partitions = null)
    {
        switch ($err) {
            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                $arrPartitions = [];
                foreach ($partitions as $val){
                    $item = [
                        'topic' => $val->getTopic(),
                        'partition' => $val->getPartition(),
                        'offset' => $val->getOffset(), 
                    ];
                    $arrPartitions[] = $item;
                }
                $msg = sprintf("partition assign :%s, %s, %s-%s", posix_getpid(), json_encode($arrPartitions), $this->topic, $this->group);
                KLog::monitor($msg, 'rebalance');
                $kafka->assign($partitions);
                $this->isRebalance = true;
                break;
            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                $arrPartitions = [];
                foreach ($partitions as $val){
                    $item = [
                        'topic' => $val->getTopic(),
                        'partition' => $val->getPartition(),
                        'offset' => $val->getOffset(),
                    ];
                    $arrPartitions[] = $item;
                }
                
                $msg = sprintf("partition revoke :%s, %s, %s-%s", posix_getpid(), json_encode($arrPartitions), $this->topic, $this->group);
                KLog::monitor($msg, 'rebalance');
                $kafka->assign(NULL);
                break;
            default:
               $msg = sprintf("rebalance fail, %s:%s, %s-%s", $err, rd_kafka_err2str($err),  $this->topic, $this->group);
               KLog::error($msg);
        }
    }
    
    public function initCallBack($conf)
    {
        $conf->setRebalanceCb([$this,'rebalanceCallBack']);
        $conf->setErrorCb([$this, 'errorCallBack']);
        $conf->setStatsCb([$this, 'statsCallBack']);
    }
    
    public function checkMetaData($consumer)
    {
        $topic = $consumer->newTopic($this->topic);
        $meta = $consumer->getMetadata(false, $topic, 10000);
    }
  
    protected function getMessageFromArr($arr)
    {
        $message = new  \RdKafka\Message; 
        $message->err = $arr['err'];
        $message->topic_name = $arr['topic_name'];
        $message->partition = $arr['partition'];
        $message->payload = $arr['payload'];
        $message->key = $arr['key'];
        $message->offset = $arr['offset'];
        return $message;
    }
    
    protected function getTopicGroupConfig()
    {
        $fileName = sprintf('%s_%s.php',$this->topic,$this->group);
        $config = require TOPIC_GROUP_CONFIG_DIR.'/'.$fileName;
        return $config;
    }
    
    //新加入一个进程，会导致所有消费者rebalance
    protected function LoadResetOffset($partition,$offset)
    {
        $cmd = "nohup  /home/work/php7/bin/php /home/work/lib/phplib/DQD/kafka/ConsumerOffsetManage.php  {$this->topic} {$this->group} {$partition}:{$offset}  >> /dev/null  2>&1 &";
        exec($cmd, $output);
    }
    
    public function execute($childObj = NULL)
    {
        try {
            global $__consumerStop, $__skipFailMsg;
            $conf = new \RdKafka\Conf();
            $conf->set('group.id', $this->group);
           
            $topicGroupConfig = $this->getTopicGroupConfig();
            
            $messageDeliveryGuarantees = isset($topicGroupConfig['message_delivery_guarantees']) ? $topicGroupConfig['message_delivery_guarantees'] : KafkaConfig::DELIVERY_GUARANTEES_NOT_MISS;
            
            if(!in_array($messageDeliveryGuarantees, [KafkaConfig::DELIVERY_GUARANTEES_NOT_REPEAT, KafkaConfig::DELIVERY_GUARANTEES_NOT_MISS, KafkaConfig::DELIVERY_GUARANTEES_EXACTLY_ONCE])) {
                $msg = 'message_delivery_guarantees value error';
                KLog::error($msg);
                echo "{$msg}\n";
                return false;
            }
            
            $consumerConf = KafkaConfig::$consumerConf;
            
            if($messageDeliveryGuarantees == KafkaConfig::DELIVERY_GUARANTEES_NOT_REPEAT) {
                $consumerConf['enable.auto.commit'] = 'true';
            } else {
                $consumerConf['enable.auto.commit'] = 'false';
            }
            
            if(isset($topicGroupConfig['consumer_conf']) && is_array($topicGroupConfig['consumer_conf'])){
                $consumerConf = array_merge($consumerConf, $topicGroupConfig['consumer_conf']);
            }
            
            foreach ($consumerConf as $key => $val){
                $conf->set($key, $val);
            }
            
            $cluster = isset($topicGroupConfig['cluster']) ? $topicGroupConfig['cluster'] : 'kafka_dqd';
            if(empty($cluster)) {
                $cluster = 'kafka_dqd';
            }
            $clusterConfigPath = "rpc.{$cluster}.brokers";
            $brokers = \Yaconf::get($clusterConfigPath);
            
            if(empty($brokers)){
                KLog::error("no broker config");
                echo "no broker config\n";
                return false;
            }
            
            $conf->set('metadata.broker.list', $brokers);
            
            $this->initCallBack($conf);
            $topicConf = new \RdKafka\TopicConf();
            
            $consumerTopicConf = KafkaConfig::$consumerTopicConf;
            
            if($messageDeliveryGuarantees == KafkaConfig::DELIVERY_GUARANTEES_NOT_REPEAT) {
                $consumerTopicConf['enable.auto.commit'] = 'true';
            } else {
                $consumerTopicConf['enable.auto.commit'] = 'false';
            }
            
            if(isset($topicGroupConfig['consumer_topic_conf']) && is_array($topicGroupConfig['consumer_topic_conf'])){
                $consumerTopicConf = array_merge($consumerTopicConf, $topicGroupConfig['consumer_topic_conf']);
            }
            
            foreach ($consumerTopicConf as $key => $val){
                $topicConf->set($key, $val);
            }
           
            $conf->setDefaultTopicConf($topicConf);
            $consumer = new \RdKafka\KafkaConsumer($conf);
            $consumer->subscribe([$this->topic]);
           
            $asyncConcurrency = isset($topicGroupConfig['async_concurrency']) ? $topicGroupConfig['async_concurrency'] : 0;
            
            $offsetCommitIntervalMs = 0;
        } catch (\Exception $e) {
            $msg = $e->getMessage();
            KLog::error($this->topic.':'.$this->group.':'.$msg);
            throw $e;
        }
        
        $message = NULL;
        $lastSuccMessage = [];
        $asyncCnt = 0;
        $offsetCommitTime = microtime(true);
        $haveNotCommitMessage = false;
        while (!$__consumerStop) {
            try {
                pcntl_signal_dispatch();//读取信号
                if($__consumerStop) {
                    $msg = sprintf("consumer exit,%s-%s-%s ", $this->topic,$this->group, posix_getpid());
                    KLog::notice($msg);
                    break;
                }
                //有可能rebanlace, 消费的partition中途变了，或消费多个partition
                //broker重启可能丢失offset
                if($message && $message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                    $lastSuccMessage[$message->partition] = $message;
                }
               
                $arrMessage = [];
                $message = $consumer->consume(KafkaConfig::$consumerPollTimeOut);
                if($message->err == RD_KAFKA_RESP_ERR_NO_ERROR) {
                    $cmpMsg = [];
                    $msgTime = date('Y-m-d H:i:s', time());
                    $flag = 0;
                    if(isset($lastSuccMessage[$message->partition])) {
                        $cmpMsg = (Array)$lastSuccMessage[$message->partition];
                        $flag = 1;
                    } else {
                        $LogName = sprintf("%s_%s_%s", $this->topic, $this->group, $message->partition);
                        $processLog = KLog::getProcessLog($LogName);
                        if(!empty($processLog)) {
                            $cmpMsg = json_decode($processLog[2], true);
                            $msgTime = $processLog[0];
                            $flag =  2;
                        }
                    }
                    
                    if(!empty($cmpMsg)) {
                        $exitMsg = '';
                        if($message->offset - $cmpMsg['offset'] > 1) {
                            $msg = json_encode($message).':'.json_encode($cmpMsg).':'.$flag;
                            KLog::monitor($msg, 'offsetexceed');
                            echo 'offset exceed:'.$msg."\n";
                        } elseif ($cmpMsg['offset'] - $message->offset > 1){
                            $msg = json_encode($message).':'.json_encode($cmpMsg).':'.$flag;
                            KLog::monitor($msg, 'offsetbehind');
                            $exitMsg = 'offset behind:'.$msg."\n";
                        } else{
                            
                        }
                        
                        if(!empty($exitMsg)) {
                            if($flag == 2){
                                if(array_key_exists('use_saved_offset_time', $topicGroupConfig)){
                                    $configTime = intval($topicGroupConfig['use_saved_offset_time']);
                                    if($configTime >=0 && strtotime($msgTime) + $configTime <= time()) {
                                        $resetMsg = $this->getMessageFromArr($cmpMsg);
                                        KLog::monitor(trim($processLog[2]), 'offsetreset');
                                        $consumer->commit($resetMsg);
                                    }
                                }
                            } 
                            echo $exitMsg;
                            $__consumerStop = true;
                            break;
                        }
                       
                    }
                } else {
                    if($haveNotCommitMessage) {
                        $diffTime = round(1000*(microtime(true) - $offsetCommitTime),1);
                        if($diffTime >= KafkaConfig::$offsetCommitIntervalMs) {
                            $consumer->commitAsync();
                            $offsetCommitTime = microtime(true);
                            $haveNotCommitMessage = false;
                        } 
                    }
                }
                
                $arrMessage[] = $message;
                foreach ($arrMessage as $message) {
                    switch ($message->err) {
                        case RD_KAFKA_RESP_ERR_NO_ERROR:
                            try {
                                $res = $this->handleMsg($message->payload, $message->partition, $message->offset, $message);
                                if($asyncConcurrency > 0) {
                                    $asyncCnt++;
                                    if($asyncCnt >= $asyncConcurrency) {
                                        $this->waitMsgComplete();
                                        $asyncCnt = 0;
                                    } 
                                }
                                
                            } catch (\Exception $e) {
                                $msg = $e->getMessage();
                                KLog::error($this->topic.':'.$this->group.':'.$msg);
                            }
                            
                            if($res || $topicGroupConfig['can_skip']) {
                                $LogName = sprintf("%s_%s_%s", $this->topic, $this->group, $message->partition);
                                KLog::writeProcessLog(json_encode($message), $LogName);
                                $lastSuccMessage[$message->partition] = $message;
                                
                                if($messageDeliveryGuarantees != KafkaConfig::DELIVERY_GUARANTEES_NOT_REPEAT) {
                                   // $diffTime = round(1000*(microtime(true) - $offsetCommitTime),1);
                                    if(true) {
                                        $consumer->commitAsync($message);
                                        $offsetCommitTime = microtime(true);
                                        $haveNotCommitMessage = false;
                                    } else {
                                        $haveNotCommitMessage = true;
                                    }
                                }
                            }
                            
                            if(!$res && $topicGroupConfig['can_skip']) {
                                KLog::monitor(json_encode($message), 'skip');//可能刷满磁盘
                            } 
                            
                            break;
                        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                            $msg = sprintf('No more messages; will wait for more, %s:%s',$this->topic, $this->group);
                            break;
                        case RD_KAFKA_RESP_ERR__TIMED_OUT:
                            $msg = sprintf('receive message Time out, %s:%s',$this->topic, $this->group);
                            break;
                        default:
                            $msg = sprintf('%s ,%s', $message->errstr(), $message->err);
                            KLog::error($msg);
                            break;
                    }
                
                }
                
            } catch (\Exception $e) {
                $msg = $e->getMessage();
                KLog::error($this->topic.':'.$this->group.':'.$msg);
                continue;
            }
            
        }
      
    }
    
}