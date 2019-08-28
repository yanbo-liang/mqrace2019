package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MessageReader {
    private static ThreadLocal<FileChannel> messageChannel = new ThreadLocal<>();
    private static ThreadLocal<FileChannel> aChannel = new ThreadLocal<>();

    static {
//        try {
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public static ByteBuffer read(ByteBuffer buffer, long tMin, long tMax) throws Exception {
        FileChannel fileChannel = messageChannel.get();
        if (fileChannel == null) {
            fileChannel = FileChannel.open(Paths.get(Constants.Message_Path), StandardOpenOption.READ);
            messageChannel.set(fileChannel);
        }
        long messageStart = PartitionIndex.getMessageStart(tMin) / Constants.Message_Size * 42;
        long messageEnd = PartitionIndex.getMessageEnd(tMax) / Constants.Message_Size * 42;
        return asyncRead(buffer, fileChannel, messageStart, messageEnd - messageStart);
    }

    public static ByteBuffer fastRead(ByteBuffer buffer, long tMin, long tMax) throws Exception {
        FileChannel fileChannel = aChannel.get();
        if (fileChannel == null) {
            fileChannel = FileChannel.open(Paths.get(Constants.A_Path), StandardOpenOption.CREATE, StandardOpenOption.READ);
            aChannel.set(fileChannel);
        }
        long aStart = PartitionIndex.getAStart(tMin);
        long aEnd = PartitionIndex.getAEnd(tMax);
       return asyncRead(buffer, fileChannel, aStart, aEnd - aStart);
    }

    private static ByteBuffer asyncRead(ByteBuffer buffer, FileChannel channel, long start, long length) throws Exception {
        long readStart = System.currentTimeMillis();
//        synchronized (buffer) {
//        buffer.limit((int) length);
//            channel.read(buffer, start, buffer, new WriteCompletionHandler());
//        channel.read(buffer, start);
//            buffer.wait();
//        }
        System.out.println("rt:\t" + (System.currentTimeMillis() - readStart));
        return channel.map(FileChannel.MapMode.READ_ONLY, start, length);

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