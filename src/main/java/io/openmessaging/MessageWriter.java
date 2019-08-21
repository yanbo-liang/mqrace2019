package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageWriter {
    private static AsynchronousFileChannel messageChannel, headerChannel;
    private static ExecutorService executor = Executors.newSingleThreadExecutor();

    private static BlockingQueue<MessageWriterTask> taskQueue = new SynchronousQueue<>();

    private static AtomicInteger pendingAsyncWrite = new AtomicInteger(0);

    private static int messageBatchSize = Constants.Message_Batch_Size;
    private static int messageSize = Constants.Message_Size;
    private static int messageBufferSize = messageBatchSize * messageSize;

    private static byte[] messageBuffer;
    private static byte[] sortMessageBuffer;

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

    public static void write(MessageWriterTask messageWriterTask) {
        try {
            taskQueue.put(messageWriterTask);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void flushAndShutDown(ByteBuffer messageBuffer, int bufferLimit) {
        try {
            write(MessageWriterTask.createEndTask(messageBuffer, bufferLimit));
            synchronized (MessageWriter.class) {
                MessageWriter.class.wait();
            }
            executor.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private static class MessageWriterJob implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    MessageWriterTask task = taskQueue.take();
                    System.out.println("remaining queue size: " + taskQueue.size());

                    if (messageBuffer == null) {
                        messageBuffer = new byte[messageBufferSize * 2];
                        sortMessageBuffer = new byte[messageBufferSize * 2];
                        System.arraycopy(task.getMessageBuffer().array(), 0, messageBuffer, messageBufferSize, messageBufferSize);
                        continue;
                    }

                    if (task.isEnd()) {
                        byte[] endMessageBuffer = new byte[messageBufferSize + task.getBufferLimit()];
                        System.arraycopy(messageBuffer, messageBufferSize, endMessageBuffer, 0, messageBufferSize);
                        System.arraycopy(task.getMessageBuffer().array(), 0, endMessageBuffer, messageBufferSize, task.getBufferLimit());
                        messageBuffer = endMessageBuffer;
                        sortMessageBuffer = new byte[messageBufferSize + task.getBufferLimit()];
                        ByteUtils.countSort(ByteBuffer.wrap(messageBuffer), sortMessageBuffer);

                        writeBatch(0, messageBufferSize, false);

                        writeBatch(messageBufferSize, task.getBufferLimit(), true);

                        DirectBufferManager.changeToRead();
                        PartitionIndex.flushIndex();

                        synchronized (MessageWriter.class) {
                            MessageWriter.class.notify();
                        }
                        break;
                    }


                    long totalStart = System.currentTimeMillis();

                    long mergeStart = System.currentTimeMillis();

                    System.arraycopy(task.getMessageBuffer().array(), 0, messageBuffer, 0, messageBufferSize);
                    System.out.println("copy time: " + (System.currentTimeMillis() - mergeStart));

                    ByteUtils.countSort(ByteBuffer.wrap(messageBuffer), sortMessageBuffer);
                    System.out.println("merge time: " + (System.currentTimeMillis() - mergeStart));


                    long writeStart = System.currentTimeMillis();
                    writeBatch(0, messageBufferSize, false);
                    System.out.println("write time:" + (System.currentTimeMillis() - writeStart));

                    byte[] tmp = messageBuffer;
                    messageBuffer = sortMessageBuffer;
                    sortMessageBuffer = tmp;

                    System.out.println("total time:" + (System.currentTimeMillis() - totalStart));
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        private void writeBatch(int start, int length, boolean isEnd) {
            long s = System.currentTimeMillis();

            ByteBuffer buffer = DirectBufferManager.borrowBuffer();
            buffer.put(sortMessageBuffer, start, length);
            buffer.flip();
            System.out.println("buffer fill " + (System.currentTimeMillis() - s));

            PartitionIndex.buildIndex(buffer);

            ByteBuffer headerBuffer = DirectBufferManager.borrowHeaderBuffer();
            for (int i = 0; i < length; i += Constants.Message_Size) {
                headerBuffer.putLong(buffer.getLong(i + 8));
            }
            headerBuffer.flip();
            asyncWrite(buffer, headerBuffer, isEnd);
            DirectBufferManager.returnBuffer(buffer);
            DirectBufferManager.returnHeaderBuffer(headerBuffer);
        }

        private void asyncWrite(ByteBuffer messageBuffer, ByteBuffer headerBuffer, boolean end) {
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

        private class WriteCompletionHandler implements CompletionHandler<Integer, AtomicInteger> {
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
}
