package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class Writer {
    BlockingDeque<WriterTask> blockingDeque = new LinkedBlockingDeque<>();
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    int count = 1;

    public class A implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    if (executorService.isShutdown()) {
                        break;
                    }
                    WriterTask task = blockingDeque.take();
                    int length = task.messages.length;
//                    Path path = Paths.get("/Users/yanbo.liang/test/" + task.start + "-" + task.end);
//                    ByteBuffer buffer = ByteBuffer.allocate(24 * length);
                    Path path = Paths.get("/alidata1/race2019/data/" + task.start + "-" + task.end);
                    ByteBuffer buffer = ByteBuffer.allocate(50 * length);
//                    buffer.limit(12 * length + 50 * length);
                    int indexPosition = 0;
                    int dataPosition = 12 * length;
                    long last = Long.MIN_VALUE;
                    int position;

                    for (Message message : task.messages) {
//                        if (last != message.getT()) {
//                            last = message.getT();
//                            position = dataPosition;
//
//                            buffer.position(indexPosition);
//                            buffer.putLong(last);
//                            buffer.putInt(position);
//                            indexPosition += 12;
//                        }
//                        buffer.position(dataPosition);

                        buffer.putLong(message.getT());
                        buffer.putLong(message.getA());
                        buffer.put(message.getBody());
//                        dataPosition += 50;
                    }
                    asyncWrite(path, buffer, task);
                }
            } catch (InterruptedException e) {
            }
        }
    }

    public Writer() {
        init();
    }

    public void write(WriterTask writerTask) {
        try {
            blockingDeque.put(writerTask);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void init() {
        executorService.execute(new A());
    }

    private void asyncWrite(Path path, ByteBuffer buffer, WriterTask task) {
        try {
            AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            buffer.flip();
            fileChannel.write(buffer, 0, buffer, new CompletionHandler<Integer, ByteBuffer>() {

                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    Reader.lowerMap.put(task.start, path);
                    Reader.upperMap.put(task.end, path);

                    task.done = true;
                    System.out.println("bytes written: " + result);
                }

                @Override
                public void failed(Throwable e, ByteBuffer attachment) {
                    System.out.println("Write failed");
                    e.printStackTrace();
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
