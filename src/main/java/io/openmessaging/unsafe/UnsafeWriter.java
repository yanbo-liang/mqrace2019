package io.openmessaging.unsafe;

import io.openmessaging.Constants;
import io.openmessaging.DirectBufferManager;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class UnsafeWriter {
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static BlockingQueue<UnsafeWriterTask> blockingQueue = new SynchronousQueue<>();
    private static UnsafeWriterJob task;
    private static AsynchronousFileChannel messageChannel, headerChannel;
    private static AtomicInteger pendingAsyncWrite = new AtomicInteger(0);
    private static long messageTotalByteWritten = 0;
    private static long headerTotalByteWritten = 0;

    static {
        try {
            messageChannel = AsynchronousFileChannel.open(Paths.get(Constants.Message_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            headerChannel = AsynchronousFileChannel.open(Paths.get(Constants.A_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            task = new UnsafeWriterJob(blockingQueue);
            executorService.execute(task);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void writeToQueue(UnsafeBuffer buffer) throws Exception {
        boolean offer = blockingQueue.offer(new UnsafeWriterTask(buffer,false), 5, TimeUnit.SECONDS);
        if (!offer) {
            System.exit(1);
            synchronized (UnsafeWriter.class){
                UnsafeWriter.class.wait();
            }
        }
    }

    static void writeToQueueEnd(UnsafeBuffer buffer) throws Exception {
        boolean offer = blockingQueue.offer(new UnsafeWriterTask(buffer,true), 5, TimeUnit.SECONDS);
        if (!offer) {
            System.exit(1);
        }
        synchronized (UnsafeWriter.class) {
            UnsafeWriter.class.wait();
        }
        executorService.shutdown();
    }

    static void asyncWrite(ByteBuffer messageBuffer, ByteBuffer headerBuffer) {
        pendingAsyncWrite.incrementAndGet();
        pendingAsyncWrite.incrementAndGet();

        messageChannel.write(messageBuffer, messageTotalByteWritten, messageBuffer, new WriteMessageHandler());
        headerChannel.write(headerBuffer, headerTotalByteWritten, headerBuffer, new WriteHeaderHandler());

        messageTotalByteWritten += messageBuffer.limit();
        headerTotalByteWritten += headerBuffer.limit();

        System.out.println("messageTotalByteWritten " + messageTotalByteWritten);
        System.out.println("headerTotalByteWritten " + headerTotalByteWritten);
    }

    static void waitAsyncWriteComplete() throws Exception {
        while (pendingAsyncWrite.get() != 0) ;
        messageChannel.close();
        headerChannel.close();
    }

    private static class WriteMessageHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer messageBuffer) {
            try {
                pendingAsyncWrite.decrementAndGet();
                DirectBufferManager.returnBuffer(messageBuffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer messageBuffer) {
            exc.printStackTrace();
        }
    }

    private static class WriteHeaderHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer headerBuffer) {
            try {
                pendingAsyncWrite.decrementAndGet();
                DirectBufferManager.returnHeaderBuffer(headerBuffer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer headerBuffer) {
            exc.printStackTrace();
        }
    }
}
