package io.openmessaging.core;

import io.openmessaging.Constants;
import io.openmessaging.DirectBufferManager;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class MessageWriter {
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static BlockingQueue<MessageBatchWrapper> blockingQueue = new SynchronousQueue<>();
    private static AtomicInteger pendingAsyncWrite = new AtomicInteger(0);
    private static AsynchronousFileChannel bodyChannel, aChannel;
    private static long bodyTotalByteWritten = 0;
    private static long aTotalByteWritten = 0;

    static {
        try {
            bodyChannel = AsynchronousFileChannel.open(Paths.get(Constants.Body_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            aChannel = AsynchronousFileChannel.open(Paths.get(Constants.A_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            executorService.execute(new MessageWriterTask(blockingQueue));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void writeToQueue(MessageBatchWrapper batchWrapper, int size) throws Exception {
        long start = System.currentTimeMillis();
        batchWrapper.size = size;
        boolean offer = blockingQueue.offer(batchWrapper, 500, TimeUnit.SECONDS);
        if (!offer) {
            System.exit(1);
        }
        System.out.println("write waited: " + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();

        synchronized (MessageWriterTask.class) {
            MessageWriterTask.class.wait();
        }
        System.out.println("copy waited: " + (System.currentTimeMillis() - start));

    }

    static void writeToQueueEnd(MessageBatchWrapper batchWrapper, int size) throws Exception {
        batchWrapper.size = size;
        batchWrapper.isEnd = true;
        boolean offer = blockingQueue.offer(batchWrapper, 500, TimeUnit.SECONDS);
        if (!offer) {
            System.exit(1);
        }
        synchronized (MessageWriter.class) {
            MessageWriter.class.wait();
        }
//        executorService.shutdown();
    }

    static void asyncWrite(ByteBuffer bodyBuffer, ByteBuffer aBuffer) {
        pendingAsyncWrite.incrementAndGet();
        pendingAsyncWrite.incrementAndGet();

        bodyChannel.write(bodyBuffer, bodyTotalByteWritten, bodyBuffer, new WriteBodyHandler());
        aChannel.write(aBuffer, aTotalByteWritten, aBuffer, new WriteAHandler());

        bodyTotalByteWritten += bodyBuffer.limit();
        aTotalByteWritten += aBuffer.limit();

        System.out.println("bodyTotalByteWritten " + bodyTotalByteWritten);
        System.out.println("aTotalByteWritten " + aTotalByteWritten);
    }

    static void waitAsyncWriteComplete() throws Exception {
        while (pendingAsyncWrite.get() != 0) ;
        bodyChannel.close();
        aChannel.close();
    }

    private static class WriteBodyHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer bodyBuffer) {
            try {
                pendingAsyncWrite.decrementAndGet();
                DirectBufferManager.returnBodyBuffer(bodyBuffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer bodyBuffer) {
            exc.printStackTrace();
        }
    }

    private static class WriteAHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer aBuffer) {
            try {
                pendingAsyncWrite.decrementAndGet();
                DirectBufferManager.returnABuffer(aBuffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer aBuffer) {
            exc.printStackTrace();
        }
    }
}
