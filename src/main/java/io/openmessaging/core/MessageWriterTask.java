package io.openmessaging.core;

import io.openmessaging.Constants;
import io.openmessaging.DirectBufferManager;
import io.openmessaging.unsafe.UnsafeWrapper;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

class MessageWriterTask implements Runnable {
    private BlockingQueue<MessageBatchWrapper> blockingQueue;
    private MessageBatchWrapper unsorted = new MessageBatchWrapper(Constants.Batch_Size * 2);
    private MessageBatchWrapper sorted = new MessageBatchWrapper(Constants.Batch_Size * 2);
    private int size = 0;
    private boolean isFirst = true;

    MessageWriterTask(BlockingQueue<MessageBatchWrapper> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                MessageBatchWrapper batchWrapper = blockingQueue.take();

                long totalStart = System.currentTimeMillis();

                boolean isEnd = batchWrapper.isEnd;

                size += batchWrapper.size;

                long start = System.currentTimeMillis();
                if (isFirst) {
                    MessageBatchWrapper.copy(batchWrapper, 0, unsorted, Constants.Batch_Size, batchWrapper.size);
                } else {
                    if (isEnd) {
                        MessageBatchWrapper.copy(unsorted, Constants.Batch_Size, unsorted, 0, Constants.Batch_Size);
                        MessageBatchWrapper.copy(batchWrapper, 0, unsorted, Constants.Batch_Size, batchWrapper.size);
                    } else {
                        MessageBatchWrapper.copy(batchWrapper, 0, unsorted, 0, batchWrapper.size);
                    }
                }
                System.out.println("copy time: " + (System.currentTimeMillis() - start));


                synchronized (MessageWriterTask.class) {
                    MessageWriterTask.class.notify();
                }


                if (isFirst) {
                    isFirst = false;
                    continue;
                }

                start = System.currentTimeMillis();
                MessageSort.countSort(unsorted, sorted, size);
                System.out.println("sort time: " + (System.currentTimeMillis() - start));


                start = System.currentTimeMillis();
                processBatch(0, Constants.Batch_Size, false);
                if (isEnd) {
                    processBatch(Constants.Batch_Size, size, true);
                    PartitionIndex.flushIndex();
                }
                System.out.println("batch time: " + (System.currentTimeMillis() - start));

                MessageBatchWrapper tmp = unsorted;
                unsorted = sorted;
                sorted = tmp;
                size -= Constants.Batch_Size;

                System.out.println("total time:" + (System.currentTimeMillis() - totalStart));
                if (isEnd) {
                    sorted = null;
                    unsorted = null;
                    synchronized (MessageWriter.class) {
                        MessageWriter.class.notify();
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

        int length = limit - start;

        ByteBuffer bodyBuffer = DirectBufferManager.borrowBodyBuffer();
        ByteBuffer aBuffer = DirectBufferManager.borrowABuffer();
        ByteBuffer sortedABuffer = DirectBufferManager.borrowSortedABuffer();

        long[] tArray = sorted.tArray;
        long[] aArray = sorted.aArray;
        byte[] bodyArray = sorted.bodyArray;

        long[] sortedAArray = new long[length];
        System.arraycopy(aArray, start, sortedAArray, 0, length);
        Arrays.sort(sortedAArray);

        for (int i = 0; i < length; i += 1) {
            PartitionIndex.buildIndex(tArray[i + start], sortedAArray[i], sortedABuffer);
            aBuffer.putLong(aArray[i]);
        }

        UnsafeWrapper.unsafeCopy(bodyArray, start, bodyBuffer, 0, length);
        bodyBuffer.position(length * Constants.Body_Size);

        bodyBuffer.flip();
        aBuffer.flip();
        sortedABuffer.flip();

        System.out.println("fill time " + (System.currentTimeMillis() - startTime));

        MessageWriter.asyncWrite(bodyBuffer, aBuffer, sortedABuffer);
        if (waitComplete) {
            MessageWriter.waitAsyncWriteComplete();
        }
    }
}
