package io.openmessaging.unsorted;

import io.openmessaging.Constants;
import io.openmessaging.unsafe.UnsafeBuffer;

import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.*;

class UnsortedWriter {
    private static ExecutorService executorService = Executors.newSingleThreadExecutor();
    private static BlockingQueue<UnsafeBuffer> blockingQueue = new ArrayBlockingQueue<>(100);
    private static AsynchronousFileChannel fileChannel;
    private static long totalByteWritten = 0;

    static {
        try {
            fileChannel = AsynchronousFileChannel.open(Paths.get(Constants.Message_Path), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            executorService.execute(new UnsortedWriterJob());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void write(UnsafeBuffer buffer) {
        try {
            blockingQueue.put(buffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class UnsortedWriterJob implements Runnable {
        @Override
        public void run() {
            try {
                while (true) {
                    UnsafeBuffer buffer = blockingQueue.take();
                    fileChannel.write(buffer.getByteBuffer(), totalByteWritten, buffer, new WriteCompletionHandler());
                    totalByteWritten += buffer.getByteBuffer().limit();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class WriteCompletionHandler implements CompletionHandler<Integer, UnsafeBuffer> {
        @Override
        public void completed(Integer result, UnsafeBuffer buffer) {
            UnsortedBufferManager.returnBuffer(buffer);
        }

        @Override
        public void failed(Throwable exc, UnsafeBuffer attachment) {
            exc.printStackTrace();
        }
    }
}
