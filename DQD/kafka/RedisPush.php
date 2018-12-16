<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2018年10月27日
*/

namespace DQD\kafka;

class RedisPush {
    
    public static $topic = NULL;
    public static $group = NULL;
    public static $topicConfig = NULL;
    public static function push($topic , $group, $data, $srcMsg, $options = [])
    {
        if(empty($data)) return true;
        $data = @json_decode($data, true);
        if(empty($data)) return true;
        
        $rpcConf = isset($data['conf']) ? $data['conf'] : [];
        $host = isset($data['host']) ? $data['host'] : '';
        $method = isset($data['method']) ? $data['method'] : '';
        $args = isset($data['args']) ? $data['args'] : [];
        
        $dataOptions = isset($data['options']) ? $data['options'] : [];
        
        $op_while_exists = isset($dataOptions['op_while_exists']) ? $dataOptions['op_while_exists'] : 0;
        $delay_time = isset($dataOptions['delay_time']) ? $dataOptions['delay_time'] : 0;
        $trans_to_del = isset($dataOptions['trans_to_del']) ? $dataOptions['trans_to_del'] : 0;
        
        //当操作需要key存在时不执行del操作
        if($op_while_exists) {
            $trans_to_del = 0;
        }
        
        if(empty($rpcConf) || empty($host) || empty($method)) return true;
        $startTime =  microtime(true);
        $objRedis = new KRedis();
        $objRedis->init($rpcConf, $host);
        
        //判断是否只有在redis key存在的情况下才进行操作
        if($op_while_exists && isset($args[0])) {
            $res = $objRedis->exists($args[0]);
            if(!$res) {
                $msg = sprintf("key not exists,not operation,%s:%s, %s,%s, %s", $topic, $group,
                    $method, json_encode($args), json_encode($srcMsg));
                KLog::monitor($msg, 'redis-request');
                return true;
            }
        }
        
        //延迟操作
        if($delay_time > 0) {
            $sleepTime = 1000* ($data['timestamp'] - microtime(true)) + $delay_time;
            if($sleepTime > 0) {
                $msg = sprintf(" sleep start, %s ms, %s:%s, %s", $sleepTime,$topic, $group, 
                    json_encode($srcMsg));
                KLog::monitor($msg, "redis-request");
                usleep($sleepTime*1000);
                $msg = sprintf(" sleep end, %s:%s, %s", $topic, $group,
                    json_encode($srcMsg));
                KLog::monitor($msg, "redis-request");
            }
        }
        
        //将操作转换为del
        if($trans_to_del && isset($args[0])) {
            $startTime =  microtime(true);
            $res = $objRedis->del($args[0]);
            $cost =  round(1000*(microtime(true) - $startTime),1);
            $msg = sprintf("=transfer to del=, %s:%s, key: %s, res: %s , %s ms, %s", $topic, $group, $args[0], $res,
                $cost, json_encode($srcMsg));
            KLog::monitor($msg, 'redis-request');
        } else {
            $startTime =  microtime(true);
            $res = call_user_func_array(array($objRedis, $method), $args);
            $cost =  round(1000*(microtime(true) - $startTime),1);
            $msg = sprintf("redis_sync, %s:%s, %s:%s, res:%s , %s ms, %s", $topic, $group, $method, 
                json_encode($args), $res, $cost, json_encode($srcMsg));
            KLog::monitor($msg, 'redis-request');
        }
        
        if($cost > 500) {
            KLog::monitor($msg, 'redis-slow');
        }
        return true;
        
    }
    
}