package io.openmessaging;

import io.openmessaging.unsafe.UnsafeBuffer;
import io.openmessaging.unsafe.UnsafeWriter;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrayPut {
    private static long[] tArray = new long[Constants.Message_Batch_Size];
    private static long[] aArray = new long[Constants.Message_Batch_Size];
    private static byte[] bodyArray = new byte[Constants.Message_Batch_Size * (Constants.Message_Size - 16)];

    private static long min = 0;
    private static long max = 0;
    private static AtomicInteger count = new AtomicInteger(0);
    private static volatile CountDownLatch latch = new CountDownLatch(Constants.Thread_Count - 1);
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
                barrier.await(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

}
