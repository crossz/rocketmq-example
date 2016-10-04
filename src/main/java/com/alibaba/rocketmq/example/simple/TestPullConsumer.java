package com.alibaba.rocketmq.example.simple;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

import java.util.*;

public class TestPullConsumer {
    private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws MQClientException {
        final DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("CZ-group");
        String topic = "TopicTest1";



        consumer.setVipChannelEnabled(false);
//        consumer.setNamesrvAddr("192.168.1.5:9876");
        consumer.setNamesrvAddr("192.168.31.58:9876");



        consumer.start();


        try {
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);


            int counter = 0;
            for (MessageQueue mq : mqs) {
                System.out.println("################ new queue of queueId: " + mq.getQueueId() + " ################");


                boolean isEmptyQ = true;
                //每个队列里无限循环，分批拉取未消费的消息，直到拉取不到新消息为止
                SINGLE_MQ:
                while (true) {


                    long offset = consumer.fetchConsumeOffset(mq, false);
                    offset = offset < 0 ? 0 : offset;



                    PullResult pullResult =
                            consumer.pullBlockIfNotFound(mq, null, offset, 20);
//                                consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);

                    System.out.println(pullResult);
                    putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            if (pullResult.getMsgFoundList() != null) {
                                int prSize = pullResult.getMsgFoundList().size();
                                System.out.println("pullResult.getMsgFoundList().size()====" + prSize);
                                if (prSize != 0) {
                                    for (MessageExt me : pullResult.getMsgFoundList()) {
                                        // 消费每条消息，如果消费失败，比如更新数据库失败，就重新再拉一次消息
                                        isEmptyQ = false;
                                        System.out.println("pullResult.getMsgFoundList()消息体内容: " + new String(me.getBody()) + " ==== queueId: " + me.getQueueId() + " ====  queueOffset: " + me.getQueueOffset());
                                    }
                                }
                            }


                            // ##### 更新消费进度 #####
                            // 获取下一个下标位置; 消费完后，更新消费进度
                            consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                            // ##### 更新消费进度 #####

                            break;




                        case NO_MATCHED_MSG:
                            break;
                        case NO_NEW_MSG:


                            // consumer.fetchConsumeOffset(mq, false) will updateOffset again, so before exiting, have to update the offset again.
                            consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
                            if (counter==0 && !isEmptyQ) Thread.sleep(1000 * 30);
                            // only counter==0 to delay 30 seconds to initialize the background thread for update offset on store.
                            counter++;



                            break SINGLE_MQ;

                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                    }
                }




                System.out.println("Store ==== queueId: " + mq.getQueueId() + " ==== offset: " + consumer.fetchConsumeOffset(mq, true));



            }
        } catch (Exception e) {
            System.err.println(e);
            e.printStackTrace();
        }




        // 定义一个定时器，用于测试pull方法时，模拟延时以便自动更新消费进度操作，生产环境中，因consumer一直在运行，因此不需要此步操作。
        final Timer timer = new Timer("TimerThread", true);
        // 定时器延时30秒后，关闭cousumer，因为客户端从首次启动时在1000*10ms即10秒后，后续每5秒定期执行一次（由参数：persistConsumerOffsetInterval 控制）向本机及broker端回写记录消费进度，
        // 因此consumer启动后需要延时至少15秒才能执行回写操作，否则下次运行pull方法时，因上次未能及时更新消费进度，程序会重复取出上次消费过的消息重新消费，所以此处延时30秒，留出回写的时间
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                consumer.shutdown();
                // 如果只要这个延迟一次，用cancel方法取消掉．
//                this.cancel();
            }
//        }, 30000);
        }, 1000 * 10);




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