<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2018年10月27日
*/


define('TOPIC_GROUP_CONFIG_DIR', dirname(__FILE__).'/topic_group_config');

date_default_timezone_set ( "Asia/Shanghai" );
require dirname(__FILE__).'/ConsumerBase.php';
require dirname(__FILE__).'/RedisPush.php';
require dirname(__FILE__).'/KRedis.php';

use DQD\kafka\ConsumerBase;
use DQD\kafka\HttpPush;
use DQD\kafka\RedisPush;

class ConsumerRedis  extends ConsumerBase {
   
    public function __construct($topic, $group)
    {
        parent::__construct($topic, $group);
    }
    
    //在此写对每一条消息的处理逻辑
    public function handleMsg($msg, $partition, $offset, $srcMsg = NULL, $options = [])
    {
        return RedisPush::push($this->topic, $this->group, $msg, $srcMsg, $options);
    }
    
    protected function getTopicGroupConfig()
    {
        $fileName = sprintf('%s_%s.php',$this->topic,$this->group);
        if(file_exists(TOPIC_GROUP_CONFIG_DIR.'/'.$fileName)){
            $config = require TOPIC_GROUP_CONFIG_DIR.'/'.$fileName;
        } else {
            $config = [
                'can_skip' => true,
                'cluster' => 'kafka_redis_sync',
            ];
        }
        $config['cluster'] =  'kafka_redis_sync';
        return $config;
    }
    
    public function waitMsgComplete()
    {
        
    }
    
}

function __consumerHelp($msg = '')
{
    $helpMsg =<<<EOD
     usage: php ConsumerRedis.php {\$topic_name}
EOD;
    
    if(!empty($msg)) echo $msg."\n";
    echo $helpMsg."\n";
    exit;
}

if($argc < 2){
    __consumerHelp("args wrong");
}


$topic = $argv[1]; //从命令行获取
$group = 'default';

if(empty($topic) || empty($group)) {
    __consumerHelp("args wrong");
}

$objConsumer = new ConsumerRedis($topic, $group);
$objConsumer->execute();

