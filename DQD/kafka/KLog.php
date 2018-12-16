<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年8月17日
*/

namespace DQD\kafka;

class KLog {
        
    public static function notice($msg)
    {
        self::writeLog($msg, 'notice');
    }
    
    public static function error($msg)
    {
        self::writeLog($msg, 'error');
    }
    
    public static function warning($msg)
    {
        self::writeLog($msg, 'warning');
    }
    
    public static function monitor($msg, $type = 'monitor', $onlyLatest = false) 
    {
        self::writeLog($msg, $type, $onlyLatest);
    }
    
    public static function writeProcessLog($msg, $type)
    {
        $file = $type . '.log';
        $path = KafkaConfig::$logpath.'/process';
        if(!file_exists($path)) {
            mkdir($path,0755);
        }
        $msg = sprintf("%s|@#|%s|@#|%s", date('Y-m-d H:i:s', time()), posix_getpid(), $msg);
        file_put_contents($path.'/'.$file, $msg . "\n");
    }
    
    public static function getProcessLog($type)
    {
        $file = $type . '.log';
        $path = KafkaConfig::$logpath.'/process/'.$file;
        if(file_exists($path)) {
            $res = file_get_contents($path);
        }
        if(empty($res)) return [];
        $res = explode('|@#|', $res);
        return $res;
        
    }
    
    public static function writeLog ($msg, $type = 'notice', $onlyLatest = false)
    {
        $bt = debug_backtrace( DEBUG_BACKTRACE_PROVIDE_OBJECT, 2);
        $msg = sprintf("[%s] [%s] [%s] [%s] [%s:%s] %s", date('Y-m-d H:i:s', time()), memory_get_usage(), $type, posix_getpid(), $bt[1]['file'], $bt[1]['line'], $msg);
        $file = $type . '.log' . '-' . date('Ymd', time());
        $path = KafkaConfig::$logpath;
        
        if(!file_exists($path)) {
            mkdir($path,0755);
        }
        
        if($onlyLatest) {
            file_put_contents($path.'/'.$file, $msg . "\n");
        } else {
            file_put_contents($path.'/'.$file, $msg . "\n", FILE_APPEND);
        }
    }
    
  
}