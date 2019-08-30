package io.openmessaging;

import io.openmessaging.unsafe.UnsafeBuffer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConcurrentMemoryCopy {
    private static int threadCount = 4;
    private static ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    private static CountDownLatch latch = new CountDownLatch(threadCount);

    private static class CopyTask implements Runnable {
        private UnsafeBuffer src;
        private UnsafeBuffer dest;
        private int srcStart;
        private int destStart;
        private int length;

        CopyTask(UnsafeBuffer src, int srcStart, UnsafeBuffer dest, int destStart, int length) {
            this.src = src;
            this.srcStart = srcStart;
            this.dest = dest;
            this.destStart = destStart;
            this.length = length;
        }

        @Override
        public void run() {
            UnsafeBuffer.copy(src, srcStart, dest, destStart, length);
            latch.countDown();
        }
    }

    public static void copy(UnsafeBuffer src, int srcStart, UnsafeBuffer dest, int destStart, int length) throws InterruptedException {
        int processed = 0;
        int batch = length / threadCount;
        int remaining = length % threadCount;
        for (int i = 0; i < threadCount; i++) {
            CopyTask copyTask;
            if (remaining != 0 && i == threadCount - 1) {
                copyTask = new CopyTask(src, srcStart + processed, dest, destStart + processed, batch + remaining);
            } else {
                copyTask = new CopyTask(src, srcStart + processed, dest, destStart + processed, batch);
            }
            processed += batch;
            executorService.execute(copyTask);
        }
        latch.await();
        latch = new CountDownLatch(threadCount);
    }
}
