package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageWriter {
    private static ExecutorService executor = Executors.newSingleThreadExecutor();
    private static BlockingQueue<MessageWriterTask> taskQueue = new SynchronousQueue<>();
    private static AsynchronousFileChannel messageChannel, headerChannel;

    private static AtomicInteger pendingAsyncWrite = new AtomicInteger(0);

    private static long[] messageBuffer = new long[Constants.Message_Buffer_Size * 2];
    private static long[] sortedMessageBuffer = new long[Constants.Message_Buffer_Size * 2];
    private static int messageBufferStart = 0;
    private static int messageBufferCount = 0;

    private static long messageTotalByteWritten = 0;
    private static long headerTotalByteWritten = 0;

    static {
        try {
            messageChannel = AsynchronousFileChannel.open(Paths.get(Constants.Message_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            headerChannel = AsynchronousFileChannel.open(Paths.get(Constants.A_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            executor.execute(new MessageWriterJob());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static class MessageWriterJob implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    System.out.println("start");
                    MessageWriterTask task = taskQueue.take();
                    messageBufferCount += task.getBufferLimit();
                    if (messageBufferStart == 0) {
                        messageBufferStart = Constants.Message_Buffer_Size;
                        long[] tmp = task.getMessageBuffer();

                        System.arraycopy(tmp, 0, messageBuffer, 0, Constants.Message_Buffer_Size);

                        continue;
                    }

                    if (task.isEnd()) {
                        long[] tmp = task.getMessageBuffer();
                        System.arraycopy(tmp, 0, messageBuffer, Constants.Message_Buffer_Size, task.getBufferLimit() * Constants.Message_Long_size);
                        LongArrayUtils.countSort(MessageWriter.messageBuffer, sortedMessageBuffer, messageBufferCount);
                        writeBatch(messageBufferCount, Constants.Message_Batch_Size, false);
                        writeBatch(messageBufferCount - Constants.Message_Batch_Size, messageBufferCount - Constants.Message_Batch_Size, true);

                        DirectBufferManager.changeToRead();
                        PartitionIndex.flushIndex();
                        synchronized (MessageWriter.class) {
                            MessageWriter.class.notify();
                        }
                        return;
                    }
                    long[] tmp1 = task.getMessageBuffer();

                    long totalStart = System.currentTimeMillis();
                    long start = System.currentTimeMillis();
                    System.arraycopy(tmp1, 0, messageBuffer, Constants.Message_Buffer_Size, Constants.Message_Buffer_Size);

                    LongArrayUtils.countSort(messageBuffer, sortedMessageBuffer, messageBufferCount);
                    System.out.println("sort time: " + (System.currentTimeMillis() - start));


                    start = System.currentTimeMillis();
                    writeBatch(messageBufferCount, Constants.Message_Batch_Size, false);
                    System.out.println("write time:" + (System.currentTimeMillis() - start));

                    long[] tmp = messageBuffer;
                    messageBuffer = sortedMessageBuffer;
                    sortedMessageBuffer = tmp;
                    messageBufferCount -= Constants.Message_Batch_Size;
                    Arrays.fill(messageBuffer, Constants.Message_Buffer_Size, Constants.Message_Buffer_Size * 2, 0);
                    System.out.println("total time:" + (System.currentTimeMillis() - totalStart));
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }
    }

    public static long[] getMessageBuffer() {
        return messageBuffer;
    }

    public static int getMessageBufferStart() {
        return messageBufferStart;
    }

    public static void write(MessageWriterTask task) {
        try {
            taskQueue.put(task);
            if (task.isEnd()){
                try {
                    synchronized (MessageWriter.class) {
                        MessageWriter.class.wait();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void writeBatch(int count, int length, boolean isEnd) {
        long start = System.currentTimeMillis();
        ByteBuffer buffer = DirectBufferManager.borrowBuffer();
        ByteBuffer headerBuffer = DirectBufferManager.borrowHeaderBuffer();

        for (int i = 0; i < length; i++) {
            int messageIndex = count - 1 - i;
            int longIndex = messageIndex * Constants.Message_Long_size;
            buffer.putLong(sortedMessageBuffer[longIndex]);
            buffer.putLong(sortedMessageBuffer[longIndex + 1]);
            headerBuffer.putLong(sortedMessageBuffer[longIndex + 1]);

            LongArrayUtils.longArraytoByteBuffer(sortedMessageBuffer, longIndex + 2, buffer);
        }
        buffer.flip();
        headerBuffer.flip();

        System.out.println("fill time " + (System.currentTimeMillis() - start));
        PartitionIndex.buildIndex(sortedMessageBuffer, count, length);

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
