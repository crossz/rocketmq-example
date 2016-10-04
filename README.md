## RocketMQ -- Alibaba's MQ [![Build Status](https://travis-ci.org/alibaba/RocketMQ.svg?branch=master)](https://travis-ci.org/alibaba/RocketMQ)

PullConsumer example:

### how to run

1. modify the namesrv_addr in the TestProducer.java: 
> roducer.setNamesrvAddr("xxx.xxx.xxx.xxx:9876");

2. modify the namesrv_addr in the TestPullConsumer.java: 
> consumer.setNamesrvAddr("xxx.xxx.xxx.xxx:9876");

## Notes:
```
// consumer.fetchConsumeOffset(mq, false) will updateOffset again, so before exiting, have to update the offset again.
consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());
if (counter==0 && !isEmptyQ) Thread.sleep(1000 * 30);
// only counter==0 to delay 30 seconds to initialize the background thread for update offset on store.
counter++;
```
