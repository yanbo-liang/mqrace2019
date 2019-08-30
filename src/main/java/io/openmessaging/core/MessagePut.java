package io.openmessaging.core;

import io.openmessaging.Constants;
import io.openmessaging.Message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MessagePut {
    private static volatile CountDownLatch latch = new CountDownLatch(Constants.Thread_Count - 1);
    private static MessageBatchWrapper batchWrapper = new MessageBatchWrapper(Constants.Batch_Size, false);
    private static AtomicInteger messageCount = new AtomicInteger(0);

    public static void put(Message message) throws Exception {
        int index = messageCount.getAndIncrement();
        if (index < Constants.Batch_Size - 1) {
            batchWrapper.putMessage(index, message);
        } else if (index == Constants.Batch_Size - 1) {
            batchWrapper.putMessage(index, message);

            latch.await(1, TimeUnit.SECONDS);

            MessageWriter.writeToQueue(batchWrapper);
            messageCount.set(0);
            synchronized (latch) {
                latch.notifyAll();
                latch = new CountDownLatch(Constants.Thread_Count - 1);
            }
        } else if (index > Constants.Batch_Size - 1) {
            synchronized (latch) {
                latch.countDown();
                latch.wait();
            }
            index = messageCount.getAndIncrement();
            batchWrapper.putMessage(index, message);
        }
    }

    public static void putEnd() throws Exception {
        MessageWriter.writeToQueueEnd(batchWrapper);
    }
}
