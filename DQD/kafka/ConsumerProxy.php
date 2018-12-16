<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年8月15日
*/


define('TOPIC_GROUP_CONFIG_DIR', dirname(__FILE__).'/topic_group_config');

date_default_timezone_set ( "Asia/Shanghai" );
require dirname(__FILE__).'/ConsumerBase.php';
require dirname(__FILE__).'/HttpPush.php';

use DQD\kafka\ConsumerBase;
use DQD\kafka\HttpPush;


class ConsumerProxy  extends ConsumerBase {
   
    public function __construct($topic, $group)
    {
        parent::__construct($topic, $group);
    }
    
    //在此写对每一条消息的处理逻辑
    public function handleMsg($msg, $partition, $offset, $srcMsg = NULL, $options = [])
    {
        $msg = [
            'm' => $msg,
            'p' => $partition,
            'o' => $offset,
        ];
        return HttpPush::push($this->topic, $this->group, $msg, $srcMsg, $options);
    }
    
    public function waitMsgComplete()
    {
        return HttpPush::waitComplete();
    }
    
}

function __consumerHelp($msg = '')
{
    $helpMsg =<<<EOD
     usage: php ConsumerProxy.php {\$topic_name} {\$group_name} 
EOD;
    
    if(!empty($msg)) echo $msg."\n";
    echo $helpMsg."\n";
    exit;
}

if($argc < 3){
    __consumerHelp("args wrong");
}


$topic = $argv[1]; //从命令行获取
$group = $argv[2];

if(empty($topic) || empty($group)) {
    __consumerHelp("args wrong");
}

$objConsumer = new ConsumerProxy($topic, $group);
$objConsumer->execute();

