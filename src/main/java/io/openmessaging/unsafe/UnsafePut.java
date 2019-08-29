package io.openmessaging.unsafe;

import io.openmessaging.Constants;
import io.openmessaging.Message;
;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class UnsafePut {
    private static UnsafeBuffer unsafeBuffer1 = new UnsafeBuffer(Constants.Message_Buffer_Size);
    private static UnsafeBuffer unsafeBuffer2 = new UnsafeBuffer(Constants.Message_Buffer_Size);
    private static UnsafeBuffer unsafeBuffer = unsafeBuffer1;

    private static AtomicInteger messageCount = new AtomicInteger(0);
    private static volatile CountDownLatch latch = new CountDownLatch(Constants.Thread_Count - 1);

    private static int batchCount = 0;

    public static void put(Message message) throws Exception {
        int count = messageCount.getAndIncrement();
        if (count < Constants.Message_Batch_Size - 1) {
            putMessage(count, message);
        } else if (count == Constants.Message_Batch_Size - 1) {
            putMessage(count, message);

            latch.await(1, TimeUnit.SECONDS);
            System.out.println(++batchCount);
            unsafeBuffer.setLimit(Constants.Message_Buffer_Size);
            long start=System.currentTimeMillis();
            UnsafeWriter.writeToQueue(unsafeBuffer);
            System.out.println("write waited: "+(System.currentTimeMillis()-start));
            if (batchCount % 2 == 1) {
                unsafeBuffer = unsafeBuffer2;
            } else {
                unsafeBuffer = unsafeBuffer1;
            }
            messageCount.getAndUpdate(x -> 0);
            synchronized (latch) {
                latch.notifyAll();
                latch = new CountDownLatch(11);
            }
        } else if (count > Constants.Message_Batch_Size - 1) {
            synchronized (latch) {
                latch.countDown();
                latch.wait();
            }
            count = messageCount.getAndIncrement();
            putMessage(count, message);
        }
    }

    public static void putEnd() throws Exception {
        unsafeBuffer.setLimit(messageCount.get() * Constants.Message_Size);
        UnsafeWriter.writeToQueueEnd(unsafeBuffer);
    }

    private static void putMessage(int count, Message message) {
        int startIndex = count * Constants.Message_Size;
        unsafeBuffer.putLong(startIndex, message.getT());
        unsafeBuffer.putLong(startIndex + 8, message.getA());
        unsafeBuffer.put(startIndex + 16, message.getBody());
    }
}
