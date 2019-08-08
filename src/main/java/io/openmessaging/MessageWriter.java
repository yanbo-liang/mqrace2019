package io.openmessaging;

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
    private AsynchronousFileChannel fileChannel;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private BlockingQueue<MessageWriterTask> taskQueue = new LinkedBlockingQueue<>(5);

    private AtomicInteger pendingAsyncWrite = new AtomicInteger(0);

    private int messageBatchSize = Constants.Message_Batch_Size;
    private int messageSize = Constants.Message_Size;
    private int messageBufferSize = messageBatchSize * messageSize;

    private byte[] messageBuffer;
    private byte[] sortMessageBuffer;

    private long totalByteWritten = 0;

    public MessageWriter() {
        try {
            Path path = Paths.get(Constants.Path);
            fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
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

                        ByteBuffer buffer = DirectBufferManager.borrowBuffer();
                        buffer.put(sortMessageBuffer, 0, messageBufferSize);
                        buffer.flip();
                        asyncWrite(buffer, false);
                        DirectBufferManager.returnBuffer(buffer);
                        buffer = DirectBufferManager.borrowBuffer();
                        buffer.put(sortMessageBuffer, messageBufferSize, task.getBufferLimit());
                        buffer.flip();
                        asyncWrite(buffer, true);
                        DirectBufferManager.returnBuffer(buffer);
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

//                    LinearExampleChecker.check(sortMessageBuffer);

                    MessageIndex.buildIndex(sortMessageBuffer, messageBufferSize);

                    long writeStart = System.currentTimeMillis();

                    ByteBuffer buffer = DirectBufferManager.borrowBuffer();
                    buffer.put(sortMessageBuffer, 0, messageBufferSize);
                    buffer.flip();
                    asyncWrite(buffer, false);
                    DirectBufferManager.returnBuffer(buffer);

                    System.out.println("write time:" + (System.currentTimeMillis() - writeStart));

                    byte[] tmp = messageBuffer;
                    messageBuffer = sortMessageBuffer;
                    sortMessageBuffer = tmp;
                    System.out.println("total time:" + (System.currentTimeMillis() - totalStart));

                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            System.out.println("end");
        }


        private void asyncWrite(ByteBuffer buffer, boolean end) {

            pendingAsyncWrite.incrementAndGet();
            fileChannel.write(buffer, totalByteWritten, fileChannel, new CompletionHandler<Integer, AsynchronousFileChannel>() {
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
                }
            });

            totalByteWritten += buffer.limit();

            if (end) {
                while (pendingAsyncWrite.get() != 0) {

                }
                synchronized (MessageWriter.class) {
                    MessageWriter.class.notify();
                }
                try {
                    fileChannel.close();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

//    public class A implements Runnable {
//        @Override
//        public void run() {
//            try {
//                while (true) {
//                    if (executorService.isShutdown()) {
//                        break;
//                    }
//                    MessageWriterTask task = taskQueue.take();
//                    int length = task.messageBuffer.length;
////                    Path Path = Paths.get("/Users/yanbo.liang/test/" + task.start + "-" + task.end);
////                    ByteBuffer buffer = ByteBuffer.allocate(24 * length);
//                    Path Path = Paths.get("/alidata1/race2019/data/" + task.start + "-" + task.end);
//                    ByteBuffer buffer = ByteBuffer.allocate(50 * length);
////                    buffer.limit(12 * length + 50 * length);
//                    int indexPosition = 0;
//                    int dataPosition = 12 * length;
//                    long last = Long.MIN_VALUE;
//                    int position;
//
//                    for (Message message : task.messageBuffer) {
////                        if (last != message.getT()) {
////                            last = message.getT();
////                            position = dataPosition;
////
////                            buffer.position(indexPosition);
////                            buffer.putLong(last);
////                            buffer.putInt(position);
////                            indexPosition += 12;
////                        }
////                        buffer.position(dataPosition);
//
//                        buffer.putLong(message.getT());
//                        buffer.putLong(message.getA());
//                        buffer.put(message.getBody());
////                        dataPosition += 50;
//                    }
//                    asyncWrite(Path, buffer, task);
//                }
//            } catch (InterruptedException e) {
//            }
//        }
//    }


//    private void asyncWrite(Path Path, ByteBuffer buffer, MessageWriterTask task) {
//        try {
//            AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(Path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
//            buffer.flip();
//            fileChannel.write(buffer, 0, buffer, new CompletionHandler<Integer, ByteBuffer>() {
//
//                @Override
//                public void completed(Integer result, ByteBuffer attachment) {
//                    MessageReader.lowerMap.put(task.start, Path);
//                    MessageReader.upperMap.put(task.end, Path);
//
//                    task.done = true;
//                    System.out.println("bytes written: " + result);
//                }
//
//                @Override
//                public void failed(Throwable e, ByteBuffer attachment) {
//                    System.out.println("Write failed");
//                    e.printStackTrace();
//                }
//            });
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
}
