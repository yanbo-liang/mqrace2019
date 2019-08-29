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
    private static UnsafeBuffer buffer=new UnsafeBuffer(8);
    private static long min = 0;
    private static long max = 0;
    private static AtomicBoolean init = new AtomicBoolean(false);
    private static CyclicBarrier barrier = new CyclicBarrier(Constants.Thread_Count, () -> {
        min += 1000;
        max += 1000;
    });

    public static void put(Message message) throws Exception {
        if (!init.get()) {
            synchronized (ArrayPut.class) {
                if (!init.get()) {
                    init.compareAndSet(false, true);
                    min = message.getT() / 1000 * 1000;
                    max = min + 999;
                }
            }
        }
        while (!(min <= message.getT() && message.getT() <= max)) {
            try {
                barrier.await(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
        buffer.putLong(0,message.getA());
//        buffer.putLong(message.getA());
//        buffer.put(message.getBody());
    }
}
