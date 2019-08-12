package io.openmessaging;

import me.lemire.integercompression.IntCompressor;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageWriter {
    private AsynchronousFileChannel messagesChannel;
    private AsynchronousFileChannel messagesWithoutDataChannel;


    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private BlockingQueue<MessageWriterTask> taskQueue = new LinkedBlockingQueue<>(5);

    private AtomicInteger pendingAsyncWrite = new AtomicInteger(0);

    private int messageBatchSize = Constants.Message_Batch_Size;
    private int messageSize = Constants.Message_Size;
    private int messageBufferSize = messageBatchSize * messageSize;

    private byte[] messageBuffer;
    private byte[] sortMessageBuffer;

    private long totalByteWritten = 0;
    private long noDataTotalByteWritten = 0;

    public MessageWriter() {
        try {
            messagesChannel = AsynchronousFileChannel.open(Paths.get(Constants.Messages), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            messagesWithoutDataChannel = AsynchronousFileChannel.open(Paths.get(Constants.Messages_Without_Data), StandardOpenOption.CREATE, StandardOpenOption.WRITE);

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
        @Override
        public void run() {
            try {
                System.out.println("start");

                while (true) {
                    System.out.println("take");
                    MessageWriterTask task = taskQueue.take();
                    System.out.println("queue size: " + taskQueue.size());

                    if (task.isEnd()) {
                        byte[] endMessageBuffer = new byte[messageBufferSize + task.getBufferLimit()];
                        System.arraycopy(messageBuffer, messageBufferSize, endMessageBuffer, 0, messageBufferSize);
                        System.arraycopy(task.getMessageBuffer(), 0, endMessageBuffer, messageBufferSize, task.getBufferLimit());
                        messageBuffer = endMessageBuffer;
                        sortMessageBuffer = new byte[messageBufferSize + task.getBufferLimit()];
                        ByteUtils.countSort(messageBuffer, sortMessageBuffer);


                        MessageIndex.buildIndex(sortMessageBuffer, messageBufferSize + task.getBufferLimit());

                        ByteBuffer noDataBuffer = DirectBufferManager.borrowBuffer();
                        for (int i = 0; i < messageBatchSize; i++) {
                            noDataBuffer.putInt((int) ByteUtils.getLong(sortMessageBuffer, i * messageSize));
                            noDataBuffer.putInt((int) ByteUtils.getLong(sortMessageBuffer, i * messageSize + 8));
                        }
                        noDataBuffer.flip();

                        ByteBuffer buffer = DirectBufferManager.borrowBuffer();
                        buffer.put(sortMessageBuffer, 0, messageBufferSize);
                        buffer.flip();
                        asyncWrite(buffer, noDataBuffer, false);
                        DirectBufferManager.returnBuffer(buffer);
                        DirectBufferManager.returnBuffer(noDataBuffer);


                        ByteBuffer noDataBuffer1 = DirectBufferManager.borrowBuffer();
                        for (int i = 0; i < task.getBufferLimit() / Constants.Message_Size; i++) {
                            noDataBuffer1.putInt((int) ByteUtils.getLong(sortMessageBuffer, i * messageSize + messageBufferSize));
                            noDataBuffer1.putInt((int) ByteUtils.getLong(sortMessageBuffer, i * messageSize + 8 + messageBufferSize));
                        }
                        noDataBuffer1.flip();

                        buffer = DirectBufferManager.borrowBuffer();
                        buffer.put(sortMessageBuffer, messageBufferSize, task.getBufferLimit());
                        buffer.flip();
                        asyncWrite(buffer, noDataBuffer1, true);
                        DirectBufferManager.returnBuffer(buffer);
                        DirectBufferManager.returnBuffer(noDataBuffer1);

                        DirectBufferManager.changeToRead();
                        synchronized (MessageWriter.class) {
                            MessageWriter.class.notify();
                        }
                        break;
                    }

                    if (messageBuffer == null) {
                        messageBuffer = new byte[messageBufferSize * 2];
                        sortMessageBuffer = new byte[messageBufferSize * 2];
                        System.arraycopy(task.getMessageBuffer(), 0, messageBuffer, messageBufferSize, messageBufferSize);
                        continue;
                    }

                    long totalStart = System.currentTimeMillis();

                    long mergeStart = System.currentTimeMillis();
                    System.arraycopy(task.getMessageBuffer(), 0, messageBuffer, 0, messageBufferSize);
                    ByteUtils.countSort(messageBuffer, sortMessageBuffer);
                    System.out.println("merge time: " + (System.currentTimeMillis() - mergeStart));


                    int[] tData = new int[messageBatchSize];
                    int[] aData = new int[messageBatchSize];

                    ByteBuffer noDataBuffer = DirectBufferManager.borrowBuffer();
                    for (int i = 0; i < messageBatchSize; i++) {
                        int t = (int) ByteUtils.getLong(sortMessageBuffer, i * messageSize);
                        int a = (int) ByteUtils.getLong(sortMessageBuffer, i * messageSize + 8);
                        noDataBuffer.putInt(t);
                        noDataBuffer.putInt(a);
                        tData[i] = t;
                        aData[i] = a;
                    }

                    IntegratedIntCompressor iic = new IntegratedIntCompressor();

                    long start = System.currentTimeMillis();
                    int[] compressed = iic.compress(tData);
                    System.out.println(System.currentTimeMillis()-start);
                    System.out.println("compressed from " + tData.length * 4 / 1024 + "KB to " + compressed.length * 4 / 1024 + "KB");
                     start = System.currentTimeMillis();
                    int[] compressed1 = iic.compress(aData);
                    System.out.println(System.currentTimeMillis()-start);
                    System.out.println("compressed from " + aData.length * 4 / 1024 + "KB to " + compressed1.length * 4 / 1024 + "KB");
                    System.exit(1);
                    noDataBuffer.flip();

//                    LinearExampleChecker.check(sortMessageBuffer);


                    MessageIndex.buildIndex(sortMessageBuffer, messageBufferSize);

                    long writeStart = System.currentTimeMillis();

                    ByteBuffer buffer = DirectBufferManager.borrowBuffer();
                    buffer.put(sortMessageBuffer, 0, messageBufferSize);
                    buffer.flip();
                    asyncWrite(buffer, noDataBuffer, false);
                    DirectBufferManager.returnBuffer(buffer);
                    DirectBufferManager.returnBuffer(noDataBuffer);

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


        private void asyncWrite(ByteBuffer buffer, ByteBuffer noDataByteBuffer, boolean end) {
            System.out.println(noDataByteBuffer.limit());
            System.out.println(noDataByteBuffer.position());

            pendingAsyncWrite.incrementAndGet();
            pendingAsyncWrite.incrementAndGet();

            messagesChannel.write(buffer, totalByteWritten, messagesChannel, new CompletionHandler<Integer, AsynchronousFileChannel>() {
                @Override
                public void completed(Integer result, AsynchronousFileChannel attachment) {
                    try {
                        pendingAsyncWrite.decrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }

                @Override
                public void failed(Throwable t, AsynchronousFileChannel attachment) {
                    t.printStackTrace();
                    System.exit(1);
                }
            });

            messagesWithoutDataChannel.write(noDataByteBuffer, noDataTotalByteWritten, messagesWithoutDataChannel, new CompletionHandler<Integer, AsynchronousFileChannel>() {
                @Override
                public void completed(Integer result, AsynchronousFileChannel attachment) {
                    try {
                        pendingAsyncWrite.decrementAndGet();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }

                @Override
                public void failed(Throwable t, AsynchronousFileChannel attachment) {
                    t.printStackTrace();
                    System.exit(1);

                }
            });

            totalByteWritten += buffer.limit();
            noDataTotalByteWritten += noDataByteBuffer.limit();
            long start = System.currentTimeMillis();
            if (end) {
                while (pendingAsyncWrite.get() != 0) {
                    if (System.currentTimeMillis() - start >= 20000) {
                        System.exit(1);
                    }
                }

                try {
                    messagesChannel.close();
                    messagesWithoutDataChannel.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
