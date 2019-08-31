package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Semaphore;

public class MessageReader {
    private static FileChannel aChannel;
    private static FileChannel bodyChannel;

    static {
        try {
            aChannel = FileChannel.open(Paths.get(Constants.A_Path), StandardOpenOption.CREATE, StandardOpenOption.READ);
            bodyChannel = FileChannel.open(Paths.get(Constants.Body_Path), StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ByteBuffer readA(ByteBuffer buffer, long tMin, long tMax) throws Exception {
        long messageStart = PartitionIndex.getMessageStart(tMin) / Constants.Message_Size * 8;
        long messageEnd = PartitionIndex.getMessageEnd(tMax) / Constants.Message_Size * 8;
        return adaptiveRead(aChannel,messageStart,messageEnd-messageStart);

    }

    public static ByteBuffer readBody(ByteBuffer buffer, long tMin, long tMax) throws Exception {
        long messageStart = PartitionIndex.getMessageStart(tMin) / Constants.Message_Size * Constants.Body_Size;
        long messageEnd = PartitionIndex.getMessageEnd(tMax) / Constants.Message_Size * Constants.Body_Size;
        return adaptiveRead(bodyChannel,messageStart,messageEnd-messageStart);

    }

    public static ByteBuffer fastRead(ByteBuffer buffer, long tMin, long tMax) throws Exception {
        long aStart = PartitionIndex.getAStart(tMin);
        long aEnd = PartitionIndex.getAEnd(tMax);
        return adaptiveRead(aChannel,aStart,aEnd-aStart);
    }
private static Semaphore semaphore = new Semaphore(5);
    private static ByteBuffer adaptiveRead(FileChannel channel, long start, long length) throws Exception {
        if (length > 1024 * 1024) {
            System.out.println("mmap:\t" + length);
            return channel.map(FileChannel.MapMode.READ_ONLY, start, length);
        } else {
            semaphore.acquire();
            long readStart = System.currentTimeMillis();
            ByteBuffer buffer = ByteBuffer.allocate((int)length);
            channel.read(buffer, start);
            buffer.flip();
            System.out.println("rt:\t" + (System.currentTimeMillis() - readStart) + "\trl:\t" + length);
            semaphore.release();
            return buffer;
        }
    }

//    private static ByteBuffer asyncRead(ByteBuffer buffer, AsynchronousFileChannel channel, long start, long length) throws Exception {
//        long readStart = System.currentTimeMillis();
//        synchronized (buffer) {
//            buffer.limit((int) length);
//            channel.read(buffer, start, buffer, new WriteCompletionHandler());
//            channel.read(buffer, start);
//            buffer.wait();
//        }
//        System.out.println("rb:\t" + length);
//
//        System.out.println("rt:\t" + (System.currentTimeMillis() - readStart));
//        return;
//
//    }

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