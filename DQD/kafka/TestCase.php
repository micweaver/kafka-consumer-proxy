<?php
/*
* @desc 
* @author lizhonghua@dongqiudi.com
* @2017年8月25日
*/

namespace DQD\kafka;

class TestCase {
    
    public function sendMsg()
    {
        
    }
    
    public function checkRes()
    {
        
        
    }
    
    public function Case()
    {
         1. producer配置错误   ****
         2. producer单topic发送，多次发送  *****
         3. producer多topic交互发送   *********
         4. producer 带key发送  **********
         5. producer ip错误
         6. broker挂掉          会hang死PHP进程     ********
//          futex(0x2d66a90, FUTEX_WAKE_PRIVATE, 1) = 0
//          futex(0x2d66abc, FUTEX_WAIT_BITSET_PRIVATE|FUTEX_CLOCK_REALTIME, 327911, {1503660727, 17987000}, ffffffff) = 0
//          futex(0x2d66a90, FUTEX_WAKE_PRIVATE, 1) = 0
         
         7. broker挂掉后恢复，消息丢失？   *******
         
         8. consumer单进程消费／多进程消费  ******
         9. consumer多group消费        ***********
         10.consumer多topic消费    ********
         11.consumer 增加   *****   会hang住消费一段时间，因为会重新分配partition
         12.consumer减少    ******
         13.命令堵住    
         14.命令堵住重启，能跳过命令    ***** 只能跳过一个命令
         15.远程消费   *****
         16.多机分布消费  ******
         17.kafka集群   **********
         18.重启不丢消息   *******可能会重， 重启可能要十多秒之后才能继续消费
         
         19. API挂掉/ip错误
         
         20. 服务器offset丢失，如何找回   ******
         21. 服务器挂了hang住php        *******
         
         22. push速度自动控制，过快或过慢
         23. partition均匀分配
         
         24. consumer挂掉可能会使用脚本看不到offset信息了    - 表示offset丢了？  *****
         25. consumer挂掉之后，服务器的offset丢失，导致从最新的开始跑数据  'auto.offset.reset' => 'smallest',*******
         26. offset不对会导致check一直失败    *****
         27. 手动重置消费点   ******

         28.检测到僵死进程
         29. 111111111111Segmentation fault (core dumped)     ******
         30. 堆积监控
         31. 大流量测试
         32. 不明原因进程hang住时，用Php5测试会打印出相关语法错误，php7不会打印错误  *******
    }
    
}