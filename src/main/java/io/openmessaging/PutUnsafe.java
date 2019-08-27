package io.openmessaging;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PutUnsafe {
    private static Unsafe unsafe;
    private static int batchSize = 5000000;
    private static UnsafeBuffer unsafeBuffer;
    private static AtomicInteger messageCount = new AtomicInteger(0);

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            unsafeBuffer = new UnsafeBuffer(batchSize * Constants.Message_Size);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class UnsafeBuffer {
        private long bufferAddress;

        UnsafeBuffer(int bufferSize) {
            bufferAddress = unsafe.allocateMemory(bufferSize);
        }

        void putLong(long index, long l) {
            unsafe.putLong(bufferAddress + index, l);
        }

        void put(int index, byte[] data) {
            for (int i = 0; i < data.length; i++) {
                unsafe.putByte(bufferAddress + index + i, data[i]);
            }
        }
    }

    private static void messageToBuffer(int count, Message message) {
        int startIndex = count * Constants.Message_Size;
        unsafeBuffer.putLong(startIndex, message.getT());
        unsafeBuffer.putLong(startIndex + 8, message.getT());
        unsafeBuffer.put(startIndex + 16, message.getBody());

    }

    private static volatile CountDownLatch latch = new CountDownLatch(11);

    static void put(Message message) {
        try {
            int count = messageCount.getAndIncrement();
            if (count < batchSize - 1) {
                messageToBuffer(count, message);

            } else if (count == batchSize - 1) {
                messageToBuffer(count, message);

                latch.await(1, TimeUnit.SECONDS);
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
