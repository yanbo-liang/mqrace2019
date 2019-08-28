package io.openmessaging.unsafe;

import io.openmessaging.Constants;
import io.openmessaging.DirectBufferManager;
import io.openmessaging.PartitionIndex;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class UnsafeWriterJob implements Runnable {
    private BlockingQueue<UnsafeWriterTask> blockingQueue;
    private UnsafeBuffer unsortedBuffer = new UnsafeBuffer(Constants.Message_Buffer_Size * 2);
    private UnsafeBuffer sortedBuffer = new UnsafeBuffer(Constants.Message_Buffer_Size * 2);
    private int sortedBufferLimit = 0;
    private boolean isFirst = true;

    UnsafeWriterJob(BlockingQueue<UnsafeWriterTask> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                UnsafeWriterTask task = blockingQueue.take();
                UnsafeBuffer buffer = task.unsafeBuffer;
                boolean isEnd = task.isEnd;

                long totalStart = System.currentTimeMillis();

                sortedBufferLimit += buffer.getLimit();

                if (isFirst) {
                    UnsafeBuffer.copy(buffer, 0, unsortedBuffer, Constants.Message_Buffer_Size, buffer.getLimit());
                    isFirst = false;
                    continue;
                } else {
                    if (isEnd) {
                        UnsafeBuffer.copy(unsortedBuffer, Constants.Message_Buffer_Size, unsortedBuffer, 0, Constants.Message_Buffer_Size);
                        UnsafeBuffer.copy(buffer, 0, unsortedBuffer, Constants.Message_Buffer_Size, buffer.getLimit());
                    } else {
                        UnsafeBuffer.copy(buffer, 0, unsortedBuffer, 0, buffer.getLimit());
                    }
                }

                long start = System.currentTimeMillis();
                UnsafeSort.countSort(unsortedBuffer, sortedBuffer, sortedBufferLimit);
                System.out.println("sort time: " + (System.currentTimeMillis() - start));


                start = System.currentTimeMillis();
                processBatch(0, Constants.Message_Buffer_Size, false);
                if (isEnd) {
                    processBatch(Constants.Message_Buffer_Size, sortedBufferLimit, true);
                }
                System.out.println("batch time: " + (System.currentTimeMillis() - start));

                UnsafeBuffer tmp = unsortedBuffer;
                unsortedBuffer = sortedBuffer;
                sortedBuffer = tmp;
                sortedBufferLimit -= Constants.Message_Buffer_Size;
                System.out.println("total time:" + (System.currentTimeMillis() - totalStart));
                if (isEnd) {
                    synchronized (UnsafeWriter.class) {
                        UnsafeWriter.class.notify();
                    }
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void processBatch(int start, int limit, boolean waitComplete) throws Exception {
        long startTime = System.currentTimeMillis();
        ByteBuffer messageBuffer = DirectBufferManager.borrowBuffer();
        ByteBuffer headerBuffer = DirectBufferManager.borrowHeaderBuffer();
        System.out.println("borrow time " + (System.currentTimeMillis() - startTime));

        startTime = System.currentTimeMillis();
        for (int i = start; i < limit; i += Constants.Message_Size) {
            messageBuffer.putLong(sortedBuffer.getLong(i));
            messageBuffer.putLong(sortedBuffer.getLong(i + 8));
            headerBuffer.putLong(sortedBuffer.getLong(i + 8));
            for (int j = 0; j < Constants.Message_Size - 16; j++) {
                messageBuffer.put(sortedBuffer.getByte(i + 16 + j));
            }
        }
        messageBuffer.flip();
        headerBuffer.flip();
        System.out.println("fill time " + (System.currentTimeMillis() - startTime));

//        PartitionIndex.buildIndex(sortedMessageBuffer, count, count);

        UnsafeWriter.asyncWrite(messageBuffer, headerBuffer);
        if (waitComplete) {
            UnsafeWriter.waitAsyncWriteComplete();
        }
    }
}
