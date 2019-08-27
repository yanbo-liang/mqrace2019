package io.openmessaging.unsafe;

import io.openmessaging.Constants;
import io.openmessaging.DirectBufferManager;
import io.openmessaging.PartitionIndex;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class UnsafeWriter {
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static BlockingQueue<UnsafeBuffer> blockingQueue = new SynchronousQueue<>();
    private static UnsafeBuffer unsortedBuffer = new UnsafeBuffer(Constants.Message_Buffer_Size * 2);
    private static UnsafeBuffer sortedBuffer = new UnsafeBuffer(Constants.Message_Buffer_Size * 2);
    private static int sortedBufferLimit = 0;
    private static boolean isFirst = true;
    private static boolean isEnd = false;
    private static AsynchronousFileChannel messageChannel, headerChannel;

    private static AtomicInteger pendingAsyncWrite = new AtomicInteger(0);

    private static long messageTotalByteWritten = 0;
    private static long headerTotalByteWritten = 0;

    static {
        try {
            messageChannel = AsynchronousFileChannel.open(Paths.get(Constants.Message_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            headerChannel = AsynchronousFileChannel.open(Paths.get(Constants.A_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            executorService.execute(new UnsafePutTask());
        } catch (Exception e) {
            e.printStackTrace();
        }

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
                    processBatch(sortedBufferLimit, Constants.Message_Buffer_Size);
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
            for (int j = 0; j < Constants.Message_Size - 16; j++) {
                buffer.put(sortedBuffer.getByte(j + 16));
            }
        }
        buffer.flip();
        headerBuffer.flip();
        System.out.println("fill time " + (System.currentTimeMillis() - start));


//        PartitionIndex.buildIndex(sortedMessageBuffer, count, count);

        asyncWrite(buffer, headerBuffer, isEnd);
        DirectBufferManager.returnBuffer(buffer);
        DirectBufferManager.returnHeaderBuffer(headerBuffer);
    }


    private static void asyncWrite(ByteBuffer messageBuffer, ByteBuffer headerBuffer, boolean end) {
        pendingAsyncWrite.incrementAndGet();
        pendingAsyncWrite.incrementAndGet();
        messageChannel.write(messageBuffer, messageTotalByteWritten, pendingAsyncWrite, new WriteCompletionHandler());
        headerChannel.write(headerBuffer, headerTotalByteWritten, pendingAsyncWrite, new WriteCompletionHandler());

        messageTotalByteWritten += messageBuffer.limit();
        headerTotalByteWritten += headerBuffer.limit();

        if (end) {
            while (pendingAsyncWrite.get() != 0) ;
            try {
                messageChannel.close();
                headerChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("messageTotalByteWritten " + messageTotalByteWritten);
        System.out.println("headerTotalByteWritten " + headerTotalByteWritten);
    }

    private static class WriteCompletionHandler implements CompletionHandler<Integer, AtomicInteger> {
        @Override
        public void completed(Integer result, AtomicInteger pendingAsyncWrite) {
            pendingAsyncWrite.decrementAndGet();
        }

        @Override
        public void failed(Throwable exc, AtomicInteger pendingAsyncWrite) {
            exc.printStackTrace();
        }
    }
}
