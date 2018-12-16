<?php
/*
 * @desc
 * @author lizhonghua@dongqiudi.com
 * @2018年10月27日
 */

namespace DQD\kafka;

class KRedis  {

    private $_redisConn = NULL;
    
    protected  $rpcConf = NULL;
    protected  $host = NULL;
 
    protected $slowTime = 500;
    
    public function init($rpcConf, $host)
    {
        $this->rpcConf = $rpcConf;
        $this->host = $host;
    }
    
    // codis架构
    public function connect()
    {
        try {
            $host = trim($this->host) ? $this->host : '127.0.0.1';
            if(strpos($host, ':')){
                list($host, $port) = explode(':', $host);
                $host = trim($host);
                $port = trim($port);
            } else {
                $port = trim($this->rpcConf['extral']['port']) ? $this->rpcConf['extral']['port'] : 6379;
            }
            $passwd = isset($this->rpcConf['extral']['passwd']) ? $this->rpcConf['extral']['passwd'] : '';
            $connTimeOut = isset($this->rpcConf['conn_timeout_ms'])?$this->rpcConf['conn_timeout_ms']/1000:1;
            $readTimeOut = isset($this->rpcConf['read_timeout_ms'])?$this->rpcConf['read_timeout_ms']/1000:1;
            $objRedis = new \Redis();
            if($connTimeOut > 0) {
                $res = $objRedis->connect($host, $port, $connTimeOut);
            } else {
                $res = $objRedis->connect($host, $port);
            }
            
            if($res === false) {
                $msg = sprintf("connect to redis fail: %s:%s, %s", $host,$port,$connTimeOut);
                KLog::error($msg);
                return false;
            }
            
            $this->_redisConn = $objRedis;
            
            if(!empty($passwd)){
                $res = $objRedis->auth($passwd); 
                if($res === false) {
                    $msg = sprintf("auth redis fail: %s:%s, %s", $host,$port,$connTimeOut);
                    KLog::error($msg);
                    return false;
                }
            }
            
            if(!empty($readTimeOut)){
                $res = $objRedis->setOption(\Redis::OPT_READ_TIMEOUT, $readTimeOut);
                if($res === false) {
                    $msg = sprintf("set read_timeout_ms fail: %s:%s, %s", $host,$port,$readTimeOut);
                    KLog::error($msg);
                    return false;
                }
            }
        
        } catch (\RedisException $re){
            $trace = $re->getTraceAsString();
            $trace = str_replace("\n", " => ", $trace);
            $msg = sprintf("%s, trace=%s",$re->getMessage(), $trace);
            KLog::error($msg);
            return false;
        }
        
    }
    
    public function __call($method, $args) 
    {
        if(!$this->_redisConn) {
             if($this->connect() === false) return NULL;
        }
        
        try{
            $res =  call_user_func_array(array($this->_redisConn, $method), $args);
        }catch (\RedisException $re){
            $trace = $re->getTraceAsString();
            $trace = str_replace("\n", " => ", $trace);
            $msg = sprintf("invoke method:%s faild, redis host:%s msg:%, trace=%s", $method, $this->host, $re->getMessage(), $trace);
            KLog::error($msg);
            return false;
        }
        
        return $res;
    }
  
    public function __destruct()
    {
        if($this->_redisConn) {
            $this->_redisConn->close();
        }
    }
    
   
}
