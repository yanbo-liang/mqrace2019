package io.openmessaging.unsafe;

import io.openmessaging.Constants;
import io.openmessaging.Message;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class UnsafePut {
    private static int batchSize = 5000000;
    private static UnsafeBuffer unsafeBuffer = new UnsafeBuffer(batchSize * Constants.Message_Size);
    private static AtomicInteger messageCount = new AtomicInteger(0);
    private static volatile CountDownLatch latch = new CountDownLatch(11);

    private static void messageToBuffer(int count, Message message) {
        int startIndex = count * Constants.Message_Size;
        unsafeBuffer.putLong(startIndex, message.getT());
        unsafeBuffer.putLong(startIndex + 8, message.getT());
        unsafeBuffer.put(startIndex + 16, message.getBody());
    }

    public static void put(Message message) {
        try {
            int count = messageCount.getAndIncrement();
            if (count < batchSize - 1) {
                messageToBuffer(count, message);

            } else if (count == batchSize - 1) {
                messageToBuffer(count, message);

                latch.await(1, TimeUnit.SECONDS);
                unsafeBuffer.free();
                unsafeBuffer = new UnsafeBuffer(batchSize * Constants.Message_Size);
                messageCount.getAndUpdate(x -> 0);
                synchronized (latch) {
                    latch.notifyAll();
                    latch = new CountDownLatch(11);
                }
            } else if (count > batchSize - 1) {
                synchronized (latch) {
                    latch.countDown();
                    latch.wait();
                }

                count = messageCount.getAndIncrement();
                messageToBuffer(count, message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
