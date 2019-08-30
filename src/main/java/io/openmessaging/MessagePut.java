package io.openmessaging;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class MessagePut {
    private static MessageBatchWrapper batchWrapper = new MessageBatchWrapper(Constants.Batch_Size, false);
    private static AtomicInteger messageCount = new AtomicInteger(0);
    private static volatile CountDownLatch latch = new CountDownLatch(Constants.Thread_Count - 1);

    public static void put(Message message) throws Exception {
        int count = messageCount.getAndIncrement();
        if (count < Constants.Batch_Size - 1) {
            putMessage(count, message);
        } else if (count == Constants.Batch_Size - 1) {
            putMessage(count, message);

            latch.await(1, TimeUnit.SECONDS);
            batchWrapper.size=Constants.Batch_Size;
            MessageWriter.writeToQueue(batchWrapper);
            messageCount.getAndUpdate(x -> 0);
            synchronized (latch) {
                latch.notifyAll();
                latch = new CountDownLatch(Constants.Thread_Count - 1);
            }
        } else if (count > Constants.Batch_Size - 1) {
            synchronized (latch) {
                latch.countDown();
                latch.wait();
            }
            count = messageCount.getAndIncrement();
            putMessage(count, message);
        }
    }

    public static void putEnd() throws Exception {
//        unsafeBuffer.setLimit(messageCount.get() * Constants.Message_Size);
//        MessageWriter.writeToQueueEnd(unsafeBuffer);
    }

    private static void putMessage(int count, Message message) {
        batchWrapper.tArray[count] = message.getT();
        batchWrapper.aArray[count] = message.getA();
        int bodyIndex = count * Constants.Body_Size;
        byte[] body = message.getBody();
//        for (int i = 0; i < Constants.Body_Size; i++) {
//            batchWrapper.bodyArray[bodyIndex + i] = body[i];
//        }
        System.arraycopy(body, 0, batchWrapper.bodyArray, bodyIndex, Constants.Body_Size);
    }
}
