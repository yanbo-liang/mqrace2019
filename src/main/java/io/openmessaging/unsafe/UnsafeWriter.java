package io.openmessaging.unsafe;

import io.openmessaging.Constants;

import java.util.concurrent.*;

public class UnsafeWriter {
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static BlockingQueue<UnsafeBuffer> blockingQueue = new SynchronousQueue<>();
    private static UnsafeBuffer unsortedBuffer = new UnsafeBuffer(Constants.Message_Buffer_Size * 2);
    private static UnsafeBuffer sortedBuffer = new UnsafeBuffer(Constants.Message_Buffer_Size * 2);
    private static int bufferLimit = 0;

    static {
        executorService.execute(new UnsafePutTask());
    }

    private static class UnsafePutTask implements Runnable {
        private static boolean isFirst = true;

        @Override
        public void run() {
            try {
                while (true) {
                    UnsafeBuffer buffer = blockingQueue.take();
                    bufferLimit += buffer.getLimit();
                    if (isFirst) {
                        UnsafeBuffer.copy(buffer, 0, unsortedBuffer, 0, buffer.getLimit());
                        isFirst=false;
                        continue;
                    } else {
                        UnsafeBuffer.copy(buffer, 0, unsortedBuffer, Constants.Message_Buffer_Size, buffer.getLimit());
                    }
                    UnsafeSort.countSort(unsortedBuffer, sortedBuffer, bufferLimit);


                    UnsafeBuffer tmp = unsortedBuffer;
                    unsortedBuffer = sortedBuffer;
                    sortedBuffer = tmp;
                    bufferLimit -= Constants.Message_Buffer_Size;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void write(UnsafeBuffer buffer) throws Exception {
        boolean offer = blockingQueue.offer(buffer, 5, TimeUnit.SECONDS);
        if (!offer){
            System.exit(1);
        }
    }
}
