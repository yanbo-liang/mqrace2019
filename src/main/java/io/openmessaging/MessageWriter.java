package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageWriter {
    private AsynchronousFileChannel messagesChannel;
    private AsynchronousFileChannel headerChannel;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private BlockingQueue<MessageWriterTask> taskQueue = new LinkedBlockingQueue<>(5);

    private AtomicInteger pendingAsyncWrite = new AtomicInteger(0);

    private int messageBatchSize = Constants.Message_Batch_Size;
    private int messageSize = Constants.Message_Size;
    private int messageBufferSize = messageBatchSize * messageSize;

    private byte[] messageBuffer;
    private byte[] sortMessageBuffer;

    private ByteBuffer unCompressedHeaderBuffer = ByteBuffer.allocate(messageBatchSize * 8);
    private ByteBuffer compressedHeaderBuffer = ByteBuffer.allocate(messageBatchSize * 8 / 2);

    private long byteWritten = 0;
    private long headerByteWritten = 0;

    public MessageWriter() {
        try {
            messagesChannel = AsynchronousFileChannel.open(Paths.get(Constants.Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            headerChannel = AsynchronousFileChannel.open(Paths.get(Constants.Header_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        } catch (IOException e) {
            e.printStackTrace();
        }
        executorService.execute(new MessageWriterJob());
    }

    public void write(MessageWriterTask messageWriterTask) {
        try {
            taskQueue.put(messageWriterTask);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void flushAndShutDown(byte[] messageBuffer, int bufferLimit) {
        try {
            write(MessageWriterTask.createEndTask(messageBuffer, bufferLimit));
            System.out.println("send end");
            synchronized (MessageWriter.class) {
                MessageWriter.class.wait();
            }
            executorService.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    private class MessageWriterJob implements Runnable {

        private ByteBuffer getCompressedHeaderBuffer(int start, int length) {
            unCompressedHeaderBuffer.clear();
            for (int i = start; i < length; i += messageSize) {
                int t = (int) ByteUtils.getLong(sortMessageBuffer, i);
                int a = (int) ByteUtils.getLong(sortMessageBuffer, i + 8);
                unCompressedHeaderBuffer.putInt(t);
                unCompressedHeaderBuffer.putInt(a-t);
            }
            byte[] uncompressed = unCompressedHeaderBuffer.array();
            byte[] compressed = compressedHeaderBuffer.array();
            int compressedSize = CompressUtil.compress(uncompressed, 0, uncompressed.length, compressed, 0, compressed.length);
            ByteBuffer buffer = DirectBufferManager.borrowSmallBuffer();
            buffer.put(compressed, 0, compressedSize);
            return buffer;
        }

        private void writeBatch(int start, int length, boolean isEnd) {
            ByteBuffer compressedHeaderBuffer = getCompressedHeaderBuffer(start, length);
            compressedHeaderBuffer.flip();
            ByteBuffer buffer = DirectBufferManager.borrowBuffer();
            buffer.put(sortMessageBuffer, start, length);
            buffer.flip();
            asyncWrite(buffer, compressedHeaderBuffer, isEnd);
            DirectBufferManager.returnBuffer(buffer);
            DirectBufferManager.returnSmallBuffer(compressedHeaderBuffer);
        }

        @Override
        public void run() {
            try {
                while (true) {
                    MessageWriterTask task = taskQueue.take();
                    System.out.println("remaining queue size: " + taskQueue.size());

                    if (messageBuffer == null) {
                        messageBuffer = new byte[messageBufferSize * 2];
                        sortMessageBuffer = new byte[messageBufferSize * 2];
                        System.arraycopy(task.getMessageBuffer(), 0, messageBuffer, messageBufferSize, messageBufferSize);
                        continue;
                    }

                    if (task.isEnd()) {
                        byte[] endMessageBuffer = new byte[messageBufferSize + task.getBufferLimit()];
                        System.arraycopy(messageBuffer, messageBufferSize, endMessageBuffer, 0, messageBufferSize);
                        System.arraycopy(task.getMessageBuffer(), 0, endMessageBuffer, messageBufferSize, task.getBufferLimit());
                        messageBuffer = endMessageBuffer;
                        sortMessageBuffer = new byte[messageBufferSize + task.getBufferLimit()];
                        ByteUtils.countSort(messageBuffer, sortMessageBuffer);

                        MessageIndex.buildIndex(sortMessageBuffer, messageBufferSize + task.getBufferLimit());

                        writeBatch(0, messageBufferSize, false);

                        writeBatch(messageBufferSize, task.getBufferLimit(), true);

                        System.out.println("header size " + headerByteWritten);
                        System.exit(1);
                        DirectBufferManager.changeToRead();
                        synchronized (MessageWriter.class) {
                            MessageWriter.class.notify();
                        }
                        break;
                    }

                    long totalStart = System.currentTimeMillis();

                    long mergeStart = System.currentTimeMillis();
                    System.arraycopy(task.getMessageBuffer(), 0, messageBuffer, 0, messageBufferSize);
                    ByteUtils.countSort(messageBuffer, sortMessageBuffer);
                    System.out.println("merge time: " + (System.currentTimeMillis() - mergeStart));

                    MessageIndex.buildIndex(sortMessageBuffer, messageBufferSize);


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
                System.exit(1);
            }

            System.out.println("end");
        }

        private void asyncWrite(ByteBuffer buffer, ByteBuffer noDataBuffer, boolean end) {
            System.out.println(noDataBuffer.limit());
            System.out.println(noDataBuffer.position());

            pendingAsyncWrite.incrementAndGet();
            pendingAsyncWrite.incrementAndGet();
            messagesChannel.write(buffer, byteWritten, pendingAsyncWrite, new WriteCompletionHandler());
            headerChannel.write(noDataBuffer, headerByteWritten, pendingAsyncWrite, new WriteCompletionHandler());

            byteWritten += buffer.limit();
            headerByteWritten += noDataBuffer.limit();

            if (end) {
                long start = System.currentTimeMillis();
                while (pendingAsyncWrite.get() != 0) {
                    if (System.currentTimeMillis() - start >= 20000) {
                        System.exit(1);
                    }
                }
                try {
                    messagesChannel.close();
                    headerChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private class WriteCompletionHandler implements CompletionHandler<Integer, AtomicInteger> {
            @Override
            public void completed(Integer result, AtomicInteger pendingAsyncWrite) {
                pendingAsyncWrite.decrementAndGet();
            }

            @Override
            public void failed(Throwable exc, AtomicInteger pendingAsyncWrite) {
                System.out.println("write failed");
                exc.printStackTrace();
                System.exit(1);
            }
        }
    }
}
