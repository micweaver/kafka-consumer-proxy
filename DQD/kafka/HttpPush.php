<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年8月16日
*/

namespace DQD\kafka;


class HttpPush {
    
    public static $topic = NULL;
    public static $group = NULL;
    public static $topicConfig = NULL;
    public static function push($topic , $group, $data, $srcMsg, $options = [])
    {
        global  $__consumerStop, $__skipFailMsg;
        
        if(self::$topicConfig == NULL) {
            $fileName = sprintf('%s_%s.php',$topic,$group);
            $config = require TOPIC_GROUP_CONFIG_DIR.'/'.$fileName;
            self::$topicConfig = $config;
        } else {
            $config = self::$topicConfig;
        }
        
        if(empty($config)) {
            $msg = sprintf("no config for %s:%s",$topic, $group); 
            KLog::error($msg);
            $__consumerStop = true;
            return false;
        }
        
        self::$topic = $topic;
        self::$group = $group;
        
        $arrHost = explode(',',trim($config['host']));
        $index = rand(0,count($arrHost) - 1);
        $host= $arrHost[$index];
        $port = $config['port'];
        $path = $config['url'];
        $retryNums = $config['retry_nums']; 
        $connTimeOut = $config['conn_timeout_ms'];
        $execTimeOut = $config['exec_timeout_ms'];
        $domain = $config['domain'];
        $canSkip = $config['can_skip'];
        $retryInterval = isset($config['retry_interval_ms'])? $config['retry_interval_ms']: 2000;
        $slowTime = isset($config['slow_time_ms']) ? $config['slow_time_ms'] : 1000;
        $asyncConcurrency = isset($config['async_concurrency']) ? $config['async_concurrency'] : 0;
        
        if($port == '80') {
            $port = '';
        } else {
            $port = ':'.$port;
        }
        
        $url = sprintf("http://%s%s/%s",$host, $port, ltrim($path,'/'));
        
        $header = [];
        if(!empty($domain)) {
            $header[] = 'host:'.$domain;
        }
        
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($data));
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
        
        if($connTimeOut > 0) {
            curl_setopt($ch, CURLOPT_CONNECTTIMEOUT_MS, $connTimeOut);
        }
        
        if($execTimeOut > 0) {
            curl_setopt($ch, CURLOPT_TIMEOUT_MS, $execTimeOut);
        }
        
        if(!empty($header)) {
            curl_setopt($ch, CURLOPT_HTTPHEADER, $header);
        }
        curl_setopt($ch, CURLOPT_URL, $url);
        
        if($asyncConcurrency > 0) {
            self::AsyncPush($ch, $url, $data);
            return true;
        }
        
        $sendSuccess = false;
        
        $step = 1;
        if(!$canSkip) $step = 0;
        $cnt = 0;
        for($i = 0 ; $i <= $retryNums; $i += $step) {
            $cnt++;
            pcntl_signal_dispatch();
            if($__consumerStop && $cnt >1){
                $msg = sprintf("consumer exit in push,%s-%s-%s ", $topic ,$group, posix_getpid());
                KLog::notice($msg);
                break;
            }
            
            if($cnt > 1) {
                if($__skipFailMsg){
                    KLog::monitor(json_encode($srcMsg), 'skip-manual');
                    $sendSuccess =  true;
                    $__skipFailMsg = false;
                    break;
                }
            }
            
            $startTime =  microtime(true);
            $result =  curl_exec($ch);
            $cost =  round(1000*(microtime(true) - $startTime),1); 
            $errno = curl_errno($ch);
            
            if($cnt > 10) {
                $msgType = 'block_'.$topic.'_'.$group.'_'.posix_getpid();
                KLog::monitor(json_encode($srcMsg), $msgType, true);
            }
           
            if($errno != CURLE_OK) {
                $errmsg = curl_error($ch);
                if($cnt < 1000) {//防止刷满磁盘
                    $msg = sprintf("msg send fail for the %s times: %s ms,  %s, %s, %s-%s  %s:%s", $cnt, $cost,  $url, json_encode($data), $topic, $group, $errno, $errmsg);
                    KLog::error($msg);
                }
                usleep($retryInterval * 1000);
                continue;
            }
            
            $sres = $result;
            $result = json_decode($result,true);
            if(!$result || $result['errno']) {
                if($cnt < 1000) {
                    $msg = sprintf("msg res invalid for the %s times: %s ms,  %s, %s, %s, %s-%s", $cnt, $cost,  $url, json_encode($data), $sres, $topic, $group);
                    KLog::error($msg);
                }
                usleep($retryInterval * 1000);
                continue;
            }
            
            if($cost > $slowTime) {
                $msg = sprintf("%s ms, %s-%s, %s, %s,%s", $cost, $topic, $group,  $url, json_encode($data), $sres);
                KLog::monitor($msg, 'push-slow');
            }
            
            $sendSuccess = true;
            break;
        }

        return $sendSuccess;
    }
    
    protected static $mh = NULL;
    protected static $arrCh = [];
    protected static $arrChInfo = [];
    public static function AsyncPush($ch, $url, $data)
    {
        if(self::$mh === NULL){
            self::$mh = curl_multi_init();
        }
        curl_multi_add_handle(self::$mh, $ch);
        self::$arrCh[] = $ch;
        self::$arrChInfo[intval($ch)]['url'] = $url;
        self::$arrChInfo[intval($ch)]['data'] = $data;
        do {
            $mrc= curl_multi_exec(self::$mh, $active);
        } while ($mrc == CURLM_CALL_MULTI_PERFORM);
        
    }
    
    public static function waitComplete()
    {
        $slowTime = isset(self::$topicConfig['slow_time_ms']) ? self::$topicConfig['slow_time_ms'] : 1000;
        $startTime =  microtime(true);
        do {
            $mrc= curl_multi_exec(self::$mh, $active);
        } while ($mrc == CURLM_CALL_MULTI_PERFORM);
        
        while ($active && $mrc == CURLM_OK) {
            if (curl_multi_select(self::$mh) != -1) {
                do {
                    $mrc = curl_multi_exec(self::$mh, $active);
                } while ($mrc == CURLM_CALL_MULTI_PERFORM);
            }
        }
        
        while($info = curl_multi_info_read(self::$mh)){
            if($info['result'] != CURLE_OK){
                $hd = intval($info['handle']);
                $url = self::$arrChInfo[$hd]['url'];
                $data = self::$arrChInfo[$hd]['data'];
                $errmsg = curl_error($info['handle']);
                $msg = sprintf("multi send fail: %s, %s, %s-%s %s:%s ", $url, json_encode($data), self::$topic, self::$group, $info['result'], $errmsg);
                KLog::error($msg);
                
                unset(self::$arrChInfo[$hd]);
                curl_multi_remove_handle(self::$mh, $info['handle']);
            }
        }
        
        foreach (self::$arrCh as $ch){
            if(empty(self::$arrChInfo[intval($ch)])) continue;
            $result = curl_multi_getcontent($ch);
            $sres = $result;
            $result = json_decode($result,true);
            if(!$result || $result['errno']) {
                $url = self::$arrChInfo[intval($ch)]['url'];
                $data = self::$arrChInfo[intval($ch)]['data'];
                $msg = sprintf("multi res invalid: %s, %s, %s, %s:%s ", $url, json_encode($data), $sres, self::$topic, self::$group);
                KLog::error($msg);
            }
            curl_multi_remove_handle(self::$mh, $ch);
        }
        curl_multi_close(self::$mh);
        
        $cost =  round(1000*(microtime(true) - $startTime),1); 
        
        if($cost >= $slowTime) {
            $msg = sprintf("%s ms, %s-%s, %s", $cost, self::$topic, self::$group,  json_encode(self::$arrChInfo));
            KLog::monitor($msg, 'push-slow-async');
        }
        
        self::$mh = NULL;
        self::$arrCh = [];
        self::$arrChInfo = [];
        
    }
    
}