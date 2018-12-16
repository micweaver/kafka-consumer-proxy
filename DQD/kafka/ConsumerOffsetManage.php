<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年9月23日
*/


define('TOPIC_GROUP_CONFIG_DIR', dirname(__FILE__).'/topic_group_config');

date_default_timezone_set ( "Asia/Shanghai" );
require dirname(__FILE__).'/ConsumerBase.php';
require dirname(__FILE__).'/HttpPush.php';

use DQD\kafka\ConsumerBase;
use DQD\kafka\HttpPush;
use DQD\kafka\KLog;
use DQD\kafka\KafkaConfig;


class ConsumerOffsetManage  extends ConsumerBase {
    
    protected $offsetInfo = [];
    public function __construct($topic, $group)
    {
        parent::__construct($topic, $group);
    }
    
    public function setOffSetInfo($offsetInfo)
    {
        $this->offsetInfo = $offsetInfo;
    }
    //在此写对每一条消息的处理逻辑
    public function handleMsg($msg, $partition, $offset, $srcMsg = NULL)
    {
        return true;
    }
    
    public function resetOffSet($consumer, $partition, $offset)
    {
        $messageArr = [
            'err' => 0,
            'topic_name' => $this->topic,
            'partition' => intval($partition),
            'payload' => '',
            'key' => NULL,
            'offset' => intval($offset),
        ];
        $message = $this->getMessageFromArr($messageArr);
        KLog::monitor('commit message:'.json_encode($message),'offsetcommit');
        $consumer->commit($message);
        return ;
    }
    
    public function execute($childObj = NULL)
    {
        try {
            global $__consumerStop;
            $conf = new \RdKafka\Conf();
            $conf->set('group.id', $this->group);
            $conf->set('enable.auto.commit','false');
            $conf->set('offset.store.method','broker');
            
            $brokers = \Yaconf::get("rpc.kafka_dqd.brokers");
            if(empty($brokers)){
                KLog::error("no broker config");
                echo "no broker config\n";
                return false;
            }
            
            $conf->set('metadata.broker.list', $brokers);
            $this->initCallBack($conf);
            
            $topicConf = new \RdKafka\TopicConf();
            $topicConf->set('enable.auto.commit','false');
            $topicConf->set('offset.store.method','broker');
            
            $conf->setDefaultTopicConf($topicConf);
            $consumer = new \RdKafka\KafkaConsumer($conf);
            $consumer->subscribe([$this->topic]);
        } catch (\Exception $e) {
            $msg = $e->getMessage();
            KLog::error($this->topic.':'.$this->group.':'.$msg);
            throw $e;
        }
        
        while (!$this->isRebalance) {
            $message = $consumer->consume(KafkaConfig::$consumerPollTimeOut);
            KLog::monitor("rebalance message:".json_encode($message),'offsetcommit');
            if($this->isRebalance) break;
        }
       
        foreach ($this->offsetInfo as $val){
              $this->resetOffSet($consumer, $val['p'], $val['o']);
        }
       
    }
    
}


function __consumerHelp($msg = '')
{
    $helpMsg =<<<EOD
     usage: php ConsumerOffsetManage.php {\$topic_name} {\$group_name} {\$partition:\$offset} ...
EOD;
    
    if(!empty($msg)) echo $msg."\n";
    echo $helpMsg."\n";
    exit;
}

if($argc < 4){
    __consumerHelp("args wrong");
}

$topic = $argv[1]; //从命令行获取
$group = $argv[2];

if(empty($topic) || empty($group)) {
    __consumerHelp("args wrong");
}

$offsetInfo = array_slice($argv, 3);
if(empty($offsetInfo)){
    __consumerHelp("need offset info");
}

$arrOffset = [];
foreach ($offsetInfo as $val){
    $oif = explode(':', $val);
    if(count($oif) !=2 || !is_numeric($oif[0]) || !is_numeric($oif[1])) {
        __consumerHelp("args '{$val}' wrong");
    }
    $arrOffset[] = [
        'p' => $oif[0],
        'o' => $oif[1],
    ];
}

$objConsumer = new ConsumerOffsetManage($topic, $group);
$objConsumer->setOffSetInfo($arrOffset);
$objConsumer->execute();

