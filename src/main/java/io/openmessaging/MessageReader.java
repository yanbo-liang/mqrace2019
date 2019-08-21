package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MessageReader {
    private static AsynchronousFileChannel messageChannel, aChannel;

    static {
        try {
            messageChannel = AsynchronousFileChannel.open(Paths.get(Constants.Message_Path), StandardOpenOption.CREATE, StandardOpenOption.READ);
            aChannel = AsynchronousFileChannel.open(Paths.get(Constants.A_Path), StandardOpenOption.CREATE, StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void read(ByteBuffer buffer, long tMin, long tMax) throws InterruptedException {
        long messageStart = PartitionIndex.getMessageStart(tMin);
        long messageEnd = PartitionIndex.getMessageEnd(tMax);
        asyncRead(buffer, messageChannel, messageStart, messageEnd - messageStart);
    }

    public static void fastRead(ByteBuffer buffer, long tMin, long tMax) throws InterruptedException {
        long aStart = PartitionIndex.getAStart(tMin);
        long aEnd = PartitionIndex.getAEnd(tMax);
        asyncRead(buffer, aChannel, aStart, aEnd - aStart);
    }

    private static void asyncRead(ByteBuffer buffer, AsynchronousFileChannel channel, long start, long length) throws InterruptedException {
        long readStart = System.currentTimeMillis();
        synchronized (buffer) {
            buffer.limit((int) length);
            channel.read(buffer, start, buffer, new WriteCompletionHandler());
            buffer.wait();
        }
        System.out.println("rt:\t" + (System.currentTimeMillis() - readStart));
    }

    private static class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer buffer) {
            synchronized (buffer) {
                System.out.println("rb:\t" + result);
                buffer.notify();
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer attachment) {
            exc.printStackTrace();
        }
    }
}