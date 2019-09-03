package io.openmessaging.core;

import io.openmessaging.Constants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MessageReader {
    private static FileChannel aChannel;
    private static FileChannel bodyChannel;
    private static ThreadLocal<ByteBuffer> aLocalBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(4 * 1024 * 1024));
    private static ThreadLocal<ByteBuffer> bodyLocalBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(4 * 1024 * 1024));
    private static ThreadLocal<RandomAccessFile> bodylocalFile = ThreadLocal.withInitial(() -> {
        try {
            return new RandomAccessFile(Constants.Body_Path, "r");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    });
    private static ThreadLocal<RandomAccessFile> alocalFile = ThreadLocal.withInitial(() -> {
        try {
            return new RandomAccessFile(Constants.A_Path, "r");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    });
    static {
        try {
            aChannel = FileChannel.open(Paths.get(Constants.A_Path), StandardOpenOption.CREATE, StandardOpenOption.READ);
            bodyChannel = FileChannel.open(Paths.get(Constants.Body_Path), StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static ByteBuffer readA(long tMin, long tMax) throws Exception {
        long min = tMin / 1000;
        long max = tMax / 1000;
        ByteBuffer byteBuffer = aLocalBuffer.get();

        long messageStart = PartitionIndex.getAStart(tMin);
        System.out.println("messageStart " + messageStart);
        long messageEnd = PartitionIndex.getAEnd(tMax);
        int length = (int) (messageEnd - messageStart);
        byteBuffer.clear();
        byteBuffer.limit(length);

        long breakpoint = -1;
        long start = System.currentTimeMillis();
        for (long i = min; i <= max; i++) {
            ByteBuffer byteBuffer1 = MessageCache.map.get(i);
            if (byteBuffer1 != null) {
                for (int j = 0; j < byteBuffer1.limit(); j++) {
                    byteBuffer.put(byteBuffer1.get(j));
                }
                length -= byteBuffer1.limit();
            } else {
                breakpoint = i;
                break;
            }
        }
        System.out.println("fill :" + (System.currentTimeMillis() - start));

        if (breakpoint != -1) {
            PartitionIndex.PartitionInfo partitionInfo = PartitionIndex.partitionMap.get(breakpoint);
            if (partitionInfo == null) {
                byteBuffer.flip();
                return byteBuffer;
            }
            messageStart = partitionInfo.aStart;
            adaptiveRead(byteBuffer, alocalFile.get(), messageStart);
        } else {
            byteBuffer.flip();
        }
        return byteBuffer;

    }

    public static ByteBuffer readBody(ByteBuffer buffer, long tMin, long tMax) throws Exception {
        long messageStart = PartitionIndex.getBodyStart(tMin);
        long messageEnd = PartitionIndex.getBodyEnd(tMax);
        ByteBuffer byteBuffer = bodyLocalBuffer.get();
        byteBuffer.clear();
        byteBuffer.limit((int) (messageEnd - messageStart));

        return adaptiveRead(byteBuffer, bodylocalFile.get(), messageStart);

    }

    //private static Semaphore semaphore = new Semaphore(1);
    private synchronized static ByteBuffer adaptiveRead(ByteBuffer byteBuffer, RandomAccessFile randomAccessFile, long start) throws Exception {
//        if (length > 1024 * 1024) {
//            System.out.println("mmap:\t" + length);
//            return channel.map(FileChannel.MapMode.READ_ONLY, start, length);
//        } else {
//            semaphore.acquire();
        long readStart = System.currentTimeMillis();
        randomAccessFile.seek(start);
        randomAccessFile.read(byteBuffer.array(), 0, byteBuffer.limit());
//        channel.read(byteBuffer, start);
        byteBuffer.position(byteBuffer.limit());
        byteBuffer.flip();
        System.out.println("rt:\t" + (System.currentTimeMillis() - readStart) + "\trl:\t");
//            semaphore.release();
        return byteBuffer;
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

//    private static class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
//        @Override
//        public void completed(Integer result, ByteBuffer buffer) {
//            synchronized (buffer) {
//                System.out.println("rb:\t" + result);
//                buffer.notify();
//            }
//        }
//
//        @Override
//        public void failed(Throwable exc, ByteBuffer attachment) {
//            exc.printStackTrace();
//        }
//    }
