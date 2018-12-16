<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年9月26日
*/

define("KAFKA_BIN_DIR", '/home/work/kafka-server/bin');
define("FILTER_MONITOR_FILE", '/home/work/logs/kafka/filter_monitor');
define("BROKER_PORT", 9092);
define('LAG_THRESHOLD', 0);

define('FIELD_TOPIC','TOPIC');
define('FIELD_PARTITION','PARTITION');
define('FIELD_CURRENT_OFFSET','CURRENT-OFFSET');
define('FIELD_LOG_END_OFFSET','LOG-END-OFFSET');
define('FIELD_LAG','LAG');
define('FIELD_HOST','HOST');

function __exit($msg)
{
    if(!empty($msg)) echo $msg."\n";
    exit;
}

function __exeCmd($cmd)
{
    exec($cmd, $result);
    return $result;
}

$ip = __exeCmd('hostname -i');
$ip = $ip[0];
$listCmd = sprintf("%s/kafka-consumer-groups.sh --bootstrap-server %s:%s --list", KAFKA_BIN_DIR, $ip, BROKER_PORT);

$groups = __exeCmd($listCmd);
$hostname = __exeCmd('hostname');
$hostname = $hostname[0];

$payload = [
    'endpoint' => $hostname,
    'metric' => 'kafka-lag',
    'timestamp' => time(),
    'value' => 0,
    'step' => 60,
    'counterType' => 'GAUGE',
    'tags' => '',
];

$filterMonitor = [];
if(file_exists(FILTER_MONITOR_FILE)) {
    $filterMonitor = file(FILTER_MONITOR_FILE, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
    $filterMonitor = is_array($filterMonitor) ? $filterMonitor : [];
}

$arrPayLoad =[];
foreach ($groups as $group){
    if(strncmp($group, 'console-consumer-', count('console-consumer-')) ==0) continue;
    $describeCmd = sprintf("%s/kafka-consumer-groups.sh --bootstrap-server %s:%s --describe --group %s", KAFKA_BIN_DIR, $ip, BROKER_PORT, $group);
    $res = __exeCmd($describeCmd);
    $indexMap = [];
    $payload['timestamp'] = time();
    foreach ($res as $val){
        $val = trim($val);
        if(empty($val)) continue;
        $arrItems =  preg_split('/\s+/', $val);
        if(in_array('TOPIC', $arrItems)){
            $indexMap = array_flip($arrItems);
            continue;
        }
        $lag = $arrItems[$indexMap[FIELD_LAG]];
        if(!is_numeric($lag)) continue;
        $payload['value'] = $lag;
        $topic = $arrItems[$indexMap[FIELD_TOPIC]];
        $tags = sprintf("topic=%s,group=%s",$topic, $group);
        if(!empty($filterMonitor) && in_array($topic.'_'.$group, $filterMonitor)) {
            continue;
        }
        if($lag >= LAG_THRESHOLD){
            if( empty($arrPayLoad[$tags])  ||  ($lag > $arrPayLoad[$tags]['value'])) {
                $payload['tags'] = $tags;
                $arrPayLoad[$tags] = $payload;
            } 
        }
    }
}

$arrPayLoad = array_values($arrPayLoad);
if(!empty($arrPayLoad)) {
    $data= json_encode($arrPayLoad);
    echo date('Y-m-d H:i:s', time()).'|'.$data."\n";
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_POST, 1);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_URL, 'http://127.0.0.1:1988/v1/push');
    $res = curl_exec($ch);
    curl_close($ch);
}

