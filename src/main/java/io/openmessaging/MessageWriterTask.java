package io.openmessaging;

import io.openmessaging.unsafe.UnsafeWrapper;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;

public class MessageWriterTask implements Runnable {
    private BlockingQueue<MessageBatchWrapper> blockingQueue;
    private MessageBatchWrapper unsorted = new MessageBatchWrapper(Constants.Batch_Size * 2, false);
    private MessageBatchWrapper sorted = new MessageBatchWrapper(Constants.Batch_Size * 2, false);
    private int size = 0;
    private boolean isFirst = true;

    MessageWriterTask(BlockingQueue<MessageBatchWrapper> blockingQueue) {
        this.blockingQueue = blockingQueue;
    }

    @Override
    public void run() {
        try {
            while (true) {
                long jobwait = System.currentTimeMillis();

                MessageBatchWrapper batchWrapper = blockingQueue.take();

                System.out.println("job wait: " + (System.currentTimeMillis() - jobwait));

                boolean isEnd = batchWrapper.isEnd;

                long totalStart = System.currentTimeMillis();

                size += batchWrapper.size;

                if (isFirst) {
                    MessageBatchWrapper.copy(batchWrapper, 0, unsorted, Constants.Batch_Size, batchWrapper.size);
                    isFirst = false;
                    synchronized (MessageWriterTask.class) {
                        MessageWriterTask.class.notify();
                    }
                    continue;
                } else {
                    if (isEnd) {
                        MessageBatchWrapper.copy(unsorted, Constants.Batch_Size, unsorted, 0, Constants.Body_Size);
                        MessageBatchWrapper.copy(batchWrapper, 0, unsorted, Constants.Batch_Size, batchWrapper.size);
                    } else {
                        MessageBatchWrapper.copy(batchWrapper, 0, unsorted, 0, batchWrapper.size);
                    }
                }
                System.out.println("copy time: " + (System.currentTimeMillis() - totalStart));
                synchronized (MessageWriterTask.class) {
                    MessageWriterTask.class.notify();
                }
                long start = System.currentTimeMillis();
                MessageSort.countSort(unsorted, sorted, size);
                System.out.println("sort time: " + (System.currentTimeMillis() - start));


                start = System.currentTimeMillis();
                processBatch(0, Constants.Batch_Size, false);
                if (isEnd) {
                    processBatch(Constants.Batch_Size, size, true);
                    PartitionIndex.flushIndex();
                    DirectBufferManager.changeToRead();
                }
                System.out.println("batch time: " + (System.currentTimeMillis() - start));

                MessageBatchWrapper tmp = unsorted;
                unsorted = sorted;
                sorted = tmp;
                size -= Constants.Batch_Size;
                System.out.println("total time:" + (System.currentTimeMillis() - totalStart));
                if (isEnd) {
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
        ByteBuffer messageBuffer = DirectBufferManager.borrowBuffer();
        ByteBuffer headerBuffer = DirectBufferManager.borrowHeaderBuffer();
        System.out.println("borrow time " + (System.currentTimeMillis() - startTime));

        startTime = System.currentTimeMillis();
        long[] tArray = sorted.tArray;
        long[] aArray = sorted.aArray;
        byte[] bodyArray = sorted.bodyArray;

        for (int i = start; i < limit; i += 1) {
            PartitionIndex.buildIndex(tArray[i]);
        }

        UnsafeWrapper.unsafeCopy(aArray, start, headerBuffer, 0,limit - start);
        UnsafeWrapper.unsafeCopy(bodyArray, start, messageBuffer, 0,limit - start);
        headerBuffer.position((limit - start)*8);
        messageBuffer.position((limit - start)*Constants.Body_Size);

        messageBuffer.flip();
        headerBuffer.flip();
        System.out.println("fill time " + (System.currentTimeMillis() - startTime));

        MessageWriter.asyncWrite(messageBuffer, headerBuffer);
        if (waitComplete) {
            MessageWriter.waitAsyncWriteComplete();
        }
    }
}
