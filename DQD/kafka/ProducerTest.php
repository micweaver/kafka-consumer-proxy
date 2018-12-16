<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年10月12日
*/

use DQD\kafka\KafkaClient;

function __TestHelp($msg = '')
{
    $helpMsg =<<<EOD
     usage: php ProducerTest.php {\$topic_name}
EOD;
    if(!empty($msg)) echo $msg."\n";
    echo $helpMsg."\n";
    exit;
}

if($argc == 2){
    $topicName = $argv[1];
} else {
    __TestHelp("wrong args");
}

require dirname(__FILE__).'/KafkaClient.php';
require dirname(__FILE__).'/KafkaConfig.php';
require dirname(__FILE__).'/KLog.php';

KafkaClient::setTopic($topicName);

$now = microtime(true);
$msg = [
   'timestamp' => $now,
   'time' => date('Y-m-d H:i:s', $now),
   'msg' => 'test msg',
];

$startTime =  microtime(true);
$res = KafkaClient::sendMsg(json_encode($msg));
$cost =  round(1000*(microtime(true) - $startTime),1); 

$result = [
    'res' => $res,
    'cost' => $cost.' ms',
];

print_r($result);