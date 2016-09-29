/**
 * 来自 RocketMQ 官网： https://github.com/alibaba/RocketMQ-docs/wiki/pullConsumer%E7%9A%84%E7%A8%8B%E5%BA%8F%E4%BE%8B%E5%AD%90
 */

package com.alibaba.rocketmq.example.simple;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;


public class TestPullConsumer {
    private static Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();


    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("please_rename_unique_group_name_5");
        String topic = "TopicTest1";

        // for rocketmq server v3.2.6
        consumer.setVipChannelEnabled(false);
        consumer.setNamesrvAddr("192.168.1.5:9876");
        // for rocketmq server v3.5.8
//        consumer.setNamesrvAddr("192.168.31.58:9876");


        consumer.setPersistConsumerOffsetInterval(1000);
        System.out.println("default persiste interval: " + consumer.getPersistConsumerOffsetInterval());



        consumer.start();




        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);

        System.out.println("mqs.size() " + mqs.size());
        // 必须加上此监听才能在消费过后，自动回写消费进度
//        consumer.registerMessageQueueListener(topic, null);
        //循环每一个队列
        for (MessageQueue mq : mqs) {

            System.out.println("######################## a new queue ########################");
            System.out.println("Consume message from queue: " + mq + " mqsize=" + mqs.size());


            boolean isEmptyQueue = true;
            int counter = 0;
            //每个队列里无限循环，分批拉取未消费的消息，直到拉取不到新消息为止
            SINGLE_MQ:
            while (counter++ < 100) {
                try {
                    long offset = consumer.fetchConsumeOffset(mq, false);
                    offset = offset < 0 ? 0 : offset;
                    System.out.println("消费进度 Offset: " + offset);
                    PullResult result = consumer.pull(mq, null, offset, 10);
                    System.out.println("接收到的消息集合" + result);


                    switch (result.getPullStatus()) {
                        case FOUND:
                            if (result.getMsgFoundList() != null) {
                                int prSize = result.getMsgFoundList().size();
                                System.out.println("pullResult.getMsgFoundList().size()===="+ prSize);
                                if (prSize != 0) {
                                    for (MessageExt me : result.getMsgFoundList()) {
                                        isEmptyQueue = false;
                                        // 消费每条消息，如果消费失败，比如更新数据库失败，就重新再拉一次消息
                                        System.out.println("pullResult.getMsgFoundList()消息体内容===="+ new String(me.getBody()) + " ==== " + me.getQueueOffset());

                                    }
                                }
                            }
                            // 获取下一个下标位置
                            offset = result.getNextBeginOffset();
                            // 消费完后，更新消费进度
                            consumer.updateConsumeOffset(mq, offset);


                            break;
                        case NO_MATCHED_MSG:
                            System.out.println("没有匹配的消息");
                            break;
                        case NO_NEW_MSG:
                            System.out.println("没有未消费的新消息");
                            //拉取不到新消息，跳出 SINGLE_MQ 当前队列循环，开始下一队列循环。


                            System.out.println("Offset from store ==== " + consumer.fetchConsumeOffset(mq, true));
                            break SINGLE_MQ;

                        case OFFSET_ILLEGAL:
                            System.out.println("下标错误");
                            break;
                        default:
                            break;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (isEmptyQueue) { } else {
                Thread.sleep(1000 * 5);
            }

        }

        consumer.shutdown();
    }

    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        offseTable.put(mq, offset);
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = offseTable.get(mq);
        if (offset != null)
            return offset;
        return 0;
    }

}