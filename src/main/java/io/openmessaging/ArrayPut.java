package io.openmessaging;

import io.openmessaging.unsafe.UnsafeBuffer;
import io.openmessaging.unsafe.UnsafeWriter;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrayPut {
    private static UnsafeBuffer buffer = new UnsafeBuffer(2000 * 100 * Constants.Message_Size);
    private static AtomicInteger count = new AtomicInteger(0);
    private static long min = 0;
    private static long max = 0;
    private static AtomicBoolean init = new AtomicBoolean(false);
    private static CyclicBarrier barrier = new CyclicBarrier(Constants.Thread_Count, () -> {
        min += 2000;
        max += 2000;
        count.set(0);
    });

    public static void put(Message message) throws Exception {
        if (!init.get()) {
            synchronized (ArrayPut.class) {
                if (!init.get()) {
                    init.compareAndSet(false, true);
                    min = message.getT() / 2000 * 2000;
                    max = min + 1999;
                }
            }
        }
        while (!(min <= message.getT() && message.getT() <= max)) {
            try {
                barrier.await(3, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        int index = count.getAndIncrement() * Constants.Message_Size;
        buffer.putLong(index, message.getT());
        buffer.putLong(index + 8, message.getA());
        buffer.put(index + 16, message.getBody());
    }
}
