<?php

/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年8月15日
*/

require dirname(__FILE__).'/KafkaConfig.php';
require dirname(__FILE__).'/KLog.php';

use DQD\kafka\KafkaConfig;
use DQD\kafka\KLog;

define('KAFKA_DIR', '/home/work/lib/phplib/DQD/kafka');
define('PROCESS_FILE_DIR',dirname(__FILE__).'/var');
define('TOPIC_GROUP_CONFIG_DIR', dirname(__FILE__).'/topic_group_config');
define('REDIS_SYNC_CONSUMER_NUM', 5);

function __controlHelp($msg = '')
{
    $helpMsg =<<<EOD
   usage:  php Control.php start|stop|restart|check|checkRedis  all| {\$topic_name} | {\$topic_name} {\$group_name}
EOD;
    
    if(!empty($msg)) echo $msg."\n";
    echo $helpMsg."\n";
    exit;
}

function __warn($msg)
{
    if(!empty($msg)) echo $msg."\n";
}


if($argc < 3) {
    __controlHelp('args wrong');
}

$config = [];
if($argc == 4) {
  $topic = $argv[2];
  $group = $argv[3];
  
  if(strstr($topic, '_') !== false || strstr($group, '_') !== false) {
      __warn("topic or group name can not contain '_'");
      exit;
  }
  
  $configFile = TOPIC_GROUP_CONFIG_DIR."/{$topic}_{$group}.php";
  if(file_exists($configFile)) {
      $config = [$topic => 
          [$group =>  require $configFile]
      ];
  } elseif(strncmp($topic, 'redis-', count('redis-')) == 0) {
      $config = [$topic =>
          [
              $group =>  [
                  'concurrency' => REDIS_SYNC_CONSUMER_NUM,
                ],
          ]
      ];
  }
  
} elseif ($argc == 3) {
    $d = dir(TOPIC_GROUP_CONFIG_DIR);
    while (false !== ($entry = $d->read())) {
        if(substr($entry, -4) != '.php') continue;
        $arrEntry = explode('_',rtrim($entry, '.php'));
        if(count($arrEntry) != 2) continue;
        
        if(strstr($argv[2], '_') !== false) {
            __warn("topic or group name can not contain '_'");
            exit;
        }
        if($argv[2] == 'all'){
            $config[$arrEntry[0]][$arrEntry[1]] = require TOPIC_GROUP_CONFIG_DIR.'/'.$entry;
        } elseif($argv[2] == $arrEntry[0]) {
            $config[$arrEntry[0]][$arrEntry[1]] = require TOPIC_GROUP_CONFIG_DIR.'/'.$entry;
        }
    }
    $d->close();
   
} else {
    __controlHelp('args wrong');
}


if(empty($config) && $argv[1]!= 'checkRedis' ) {
    __warn("no kafka config for topic '{$argv[2]}' ");
    exit;
}

switch (strtolower($argv[1])) {
    case 'start':
        start();
        break;
    case 'stop':
        stop();
        break;
    case 'restart':
        restart();
        break;
    case 'check':
        check();
        break;
    case 'checkredis':
        checkRedis();
        break;
    default:
       __controlHelp("args wrong") ;
    break;
}


exit("==========completed==========\n");

function getProcessFile($topic, $group)
{
    $fileName = sprintf('%s_%s.process',$topic,$group);
    $filePath = PROCESS_FILE_DIR.'/'.$fileName;
    return $filePath;
}

function getProcessIds($topic, $group)
{
    $filePath = getProcessFile($topic, $group);
    if(file_exists($filePath)){
        $cont = file_get_contents($filePath);
        $processIds = explode("\n", trim($cont));
        return $processIds;
    }
    return [];
}

function writeProcessId($topic, $group, $proccessId)
{
    $filePath = getProcessFile($topic, $group);
    file_put_contents($filePath, "{$proccessId}\n",FILE_APPEND);
}

function start()
{
    global $config;
    pcntl_signal(SIGCHLD, SIG_IGN);
    foreach ($config as $topic => $groups){
        foreach ($groups as $group => $val) {
            if(empty($group) || empty($topic)) continue;
            $processIds = getProcessIds($topic, $group);
            $concurrency = $val['concurrency'];
            $nowNum = count($processIds);
            if($nowNum >= $concurrency) {
                __warn("{$topic} {$group} has start, num:{$nowNum}");
                continue;
            }
            $num = $concurrency - $nowNum;
            for($i = 0 ; $i < $num; $i++){
                $pid = LoadConsumer($topic, $group);
                if($pid){
                    __warn("Load {$topic} {$group} {$i} {$pid} success");
                } else {
                    __warn("Load {$topic} {$group} {$i} {$pid} fail");
                }
            }
        }
    }

}

function stop($delete = false)
{
    global $config;
    foreach ($config as $topic => $groups){
        foreach ($groups as $group => $val) {
            if(empty($group) || empty($topic)) continue;
            $processIds = getProcessIds($topic, $group);
            if(empty($processIds)) {
                __warn("{$topic} {$group} has stoped");
                if($delete) {
                    unset($config[$topic][$group]);
                }
                continue;
            }
            
            foreach ($processIds as $processId) {
                if(posix_kill($processId, SIGTERM)){
                    __warn("stop {$topic} {$group} {$processId} success");
                } else {
                    __warn("stop {$topic} {$group} {$processId} fail");
                }
            }
            
            $processFile = getProcessFile($topic, $group);
            $newDir = dirname($processFile).'/bak';
            if(!file_exists($newDir)) {
                mkdir($newDir,0700);
            }
            $newFile = $newDir.'/'.basename($processFile).'.'.time();
            rename($processFile, $newFile);
         }
    }
    
}

function restart()
{
    stop(true);
    echo "stop waiting...\n";
    sleep(6);
    start();
}

//pid不能做为检查的依据，会重复使用
function check()
{
    $cmd = "ps -eo pid,comm,state,args | grep -i 'ConsumerOffsetManage.php' | grep -v grep";
    exec($cmd, $arrResOffset);
    if(!empty($arrResOffset))  {
        KLog::monitor(json_encode($arrResOffset),'offsetManage');
        return ;
    }
    
    $cmd = "ps -eo pid,comm,state,args | grep -i 'ConsumerProxy.php' | grep -v grep";
    exec($cmd, $arrRes);
    $arrPid = [];
    foreach ($arrRes as $val){
       $arrLine = preg_split('/\s+/', trim($val));
       if($arrLine[1] != 'php' || $arrLine[3] !='/home/work/php7/bin/php' || $arrLine[4] != KAFKA_DIR.'/ConsumerProxy.php') continue;
       $arrPid[$arrLine[5]][$arrLine[6]][] = $arrLine[0];
    }
    
    global $config;
    $checkConfig = $config;
    foreach ($checkConfig as $topic => $groups){
        foreach ($groups as $group => $val) {
            if(!empty($arrPid[$topic][$group])) {
                $alivePids = $arrPid[$topic][$group];
            } else {
                $alivePids = [];
            }
            $historyPis = getProcessIds($topic, $group);
            if(empty($historyPis)) continue;
            $diffPids= array_diff($historyPis, $alivePids);
            if(!empty($diffPids)) {
                $msg = sprintf("%s-%s,%s,%s", $topic, $group, json_encode($alivePids), json_encode($historyPis));
                KLog::monitor($msg, 'check');
                $oldConfig = $config;
                $newConfig = [];
                $newConfig[$topic][$group] = $config[$topic][$group];
                $config = $newConfig;
                restart();
                $config = $oldConfig;
            } else {
                //echo date('Y-m-d H:i:s', time()).":{$topic}-{$group}, check success\n";
            }
        }
    }
   
}


//pid不能做为检查的依据，会重复使用
function checkRedis()
{
    $cmd = "ps -eo pid,comm,state,args | grep -i 'ConsumerOffsetManage.php' | grep -v grep";
    exec($cmd, $arrResOffset);
    if(!empty($arrResOffset))  {
        KLog::monitor(json_encode($arrResOffset),'offsetManage');
        return ;
    }
    
    //redis同步消费者
    $cmd = "ps -eo pid,comm,state,args | grep -i 'ConsumerRedis.php' | grep -v grep";
    exec($cmd, $arrRes);
    $arrPid = [];
    foreach ($arrRes as $val){
        $arrLine = preg_split('/\s+/', trim($val));
        if($arrLine[1] != 'php' || $arrLine[3] !='/home/work/php7/bin/php' || $arrLine[4] != KAFKA_DIR.'/ConsumerRedis.php') continue;
        $arrPid[$arrLine[5]][$arrLine[6]][] = $arrLine[0];
    }
    
    global $config;
    $d = dir(PROCESS_FILE_DIR);
    while (false !== ($entry = $d->read())) {
        if(substr($entry, -8) != '.process') continue;
        $arrEntry = explode('_',rtrim($entry, '.process'));
        if(count($arrEntry) != 2) continue;
        $topic = $arrEntry[0];
        $group = $arrEntry[1];
        if(!empty($arrPid[$topic][$group])) {
            $alivePids = $arrPid[$topic][$group];
        } else {
            $alivePids = [];
        }
        
        $historyPis = getProcessIds($topic, $group);
        if(empty($historyPis)) continue;
        $diffPids= array_diff($historyPis, $alivePids);
        if(!empty($diffPids)) {
            $msg = sprintf("%s-%s,%s,%s", $topic, $group, json_encode($alivePids), json_encode($historyPis));
            KLog::monitor($msg, 'check');
            $oldConfig = $config;
            $newConfig = [];
            $newConfig[$topic][$group] =  [
                'concurrency' => REDIS_SYNC_CONSUMER_NUM,
            ];
            $config = $newConfig;
            restart();
            $config = $oldConfig;
        } else {
            //echo date('Y-m-d H:i:s', time()).":{$topic}-{$group}, check success\n";
        }
    
    }
    
}


function LoadConsumer($topic, $group)
{
    $pid = pcntl_fork();
    if ($pid == -1) {
        __warn("{$topic}-{$group} fork fail");
        return false;
    } else if ($pid) {
        // we are the parent
       // pcntl_wait($status); //Protect against Zombie children
        if(!file_exists(PROCESS_FILE_DIR)) {
            mkdir(PROCESS_FILE_DIR, 0700);
        }
        writeProcessId($topic, $group, $pid);
        return $pid;
    } else {
        // we are the child
        $bin = "/home/work/php7/bin/php";
        if(strncmp($topic, 'redis-', count('redis-')) == 0) {
            $args = [ KAFKA_DIR.'/ConsumerRedis.php', $topic, $group];
        } else {
            $args = [ KAFKA_DIR.'/ConsumerProxy.php', $topic, $group];
        }
        
        if(!pcntl_exec($bin, $args)) {
            __warn("{$topic}-{$group} exec fail");
            return false;
        }
    }
    
    return true;
}