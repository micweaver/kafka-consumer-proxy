<?php
use DQD\kafka\KafkaClient;
require 'KafkaClient.php';
require 'KafkaConfig.php';
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2018年6月21日
*/


KafkaClient::init('127.0.0.1', '/home/work/logs/kafka');
KafkaClient::setTopic('test');
KafkaClient::sendMsg("test msg");