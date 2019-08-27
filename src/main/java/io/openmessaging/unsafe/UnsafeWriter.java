package io.openmessaging.unsafe;

import io.openmessaging.Constants;
import io.openmessaging.DirectBufferManager;
import io.openmessaging.PartitionIndex;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

public class UnsafeWriter {
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static BlockingQueue<UnsafeBuffer> blockingQueue = new SynchronousQueue<>();
    private static UnsafeBuffer unsortedBuffer = new UnsafeBuffer(Constants.Message_Buffer_Size * 2);
    private static UnsafeBuffer sortedBuffer = new UnsafeBuffer(Constants.Message_Buffer_Size * 2);
    private static int sortedBufferLimit = 0;
    private static boolean isFirst = true;
    private static boolean isEnd = false;

    static {
        executorService.execute(new UnsafePutTask());
    }

    private static class UnsafePutTask implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    UnsafeBuffer buffer = blockingQueue.take();

                    long totalStart = System.currentTimeMillis();
                    sortedBufferLimit += buffer.getLimit();
                    if (isFirst) {
                        UnsafeBuffer.copy(buffer, 0, unsortedBuffer, 0, buffer.getLimit());
                        isFirst = false;
                        continue;
                    } else {
                        UnsafeBuffer.copy(buffer, 0, unsortedBuffer, Constants.Message_Buffer_Size, buffer.getLimit());
                    }
                    if (isEnd) {

                    }

                    long start = System.currentTimeMillis();
                    UnsafeSort.countSort(unsortedBuffer, sortedBuffer, sortedBufferLimit);
                    System.out.println("sort time: " + (System.currentTimeMillis() - start));


                    start = System.currentTimeMillis();
                    processBatch(sortedBufferLimit,Constants.Message_Buffer_Size);
                    System.out.println("batch time: " + (System.currentTimeMillis() - start));

                    UnsafeBuffer tmp = unsortedBuffer;
                    unsortedBuffer = sortedBuffer;
                    sortedBuffer = tmp;
                    sortedBufferLimit -= Constants.Message_Buffer_Size;
                    System.out.println("total time:" + (System.currentTimeMillis() - totalStart));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void processBatch(int limit, int last) throws Exception {
        long start = System.currentTimeMillis();
        ByteBuffer buffer = DirectBufferManager.borrowBuffer();
        ByteBuffer headerBuffer = DirectBufferManager.borrowHeaderBuffer();
        System.out.println("borrow time " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();
        for (int i = limit - Constants.Message_Size; i >= last; i -= Constants.Message_Size) {
            buffer.putLong(sortedBuffer.getLong(i));
            buffer.putLong(sortedBuffer.getLong(i + 8));
            headerBuffer.putLong(sortedBuffer.getLong(i + 8));
            for (int j = i + 16; j < Constants.Message_Size; j++) {
                buffer.put(sortedBuffer.getByte(j));
            }
        }
        buffer.flip();
        headerBuffer.flip();
        System.out.println("fill time " + (System.currentTimeMillis() - start));


//        PartitionIndex.buildIndex(sortedMessageBuffer, count, count);

//        asyncWrite(buffer, headerBuffer, isEnd);
        DirectBufferManager.returnBuffer(buffer);
        DirectBufferManager.returnHeaderBuffer(headerBuffer);
    }

    public static void write(UnsafeBuffer buffer) throws Exception {
        boolean offer = blockingQueue.offer(buffer, 5, TimeUnit.SECONDS);
        if (!offer) {
            System.exit(1);
        }
    }

    public static void writeEnd(UnsafeBuffer buffer) throws Exception {
        isEnd = true;
        boolean offer = blockingQueue.offer(buffer, 5, TimeUnit.SECONDS);
        if (!offer) {
            System.exit(1);
        }
    }
}
