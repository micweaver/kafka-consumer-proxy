<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年10月12日
*/

define('TOPIC_GROUP_CONFIG_DIR', dirname(__FILE__).'/topic_group_config');

date_default_timezone_set ( "Asia/Shanghai" );
require dirname(__FILE__).'/ConsumerBase.php';
require dirname(__FILE__).'/HttpPush.php';

use DQD\kafka\ConsumerBase;
use DQD\kafka\HttpPush;


class ConsumerTest  extends ConsumerBase {
    
    public function __construct($topic, $group)
    {
        parent::__construct($topic, $group);
    }
    
    public function handleMsg($msg, $partition, $offset, $srcMsg = NULL)
    {
        $now = microtime(true);
        $payLoad = json_decode($srcMsg->payload,true);
        $queueTime = round(1000*($now - $payLoad['timestamp']),1);
        $msg = [
            'msg' => $srcMsg,
            'timestamp' => $now,
            'time' => date('Y-m-d H:i:s', $now),
            'queueTime' => $queueTime.' ms',
        ];
        print_r($msg);
        return true;
    }
    
}

function __consumerHelp($msg = '')
{
    $helpMsg =<<<EOD
     usage: php ConsumerTest.php {\$topic_name} {\$group_name}
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

$objConsumer = new ConsumerTest($topic, $group);
$objConsumer->execute();