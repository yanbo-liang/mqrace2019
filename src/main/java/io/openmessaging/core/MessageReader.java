package io.openmessaging.core;

import io.openmessaging.Constants;
import io.openmessaging.Message;
import io.openmessaging.core.partition.APartition;
import io.openmessaging.core.partition.MessagePartition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class MessageReader {
    private static FileChannel aChannel, sortedAChannel, bodyChannel;
    private static ThreadLocal<ByteBuffer> aLocalBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(10 * 1024 * 1024));
    private static ThreadLocal<ByteBuffer> bodyLocalBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(10 * 1024 * 1024));

    static {
        try {
            aChannel = FileChannel.open(Paths.get(Constants.A_Path), StandardOpenOption.CREATE, StandardOpenOption.READ);
            sortedAChannel = FileChannel.open(Paths.get(Constants.Sorted_A_Path), StandardOpenOption.CREATE, StandardOpenOption.READ);
            bodyChannel = FileChannel.open(Paths.get(Constants.Body_Path), StandardOpenOption.READ);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void read(ByteBuffer buffer, FileChannel channel, long start, long length) throws Exception {
        buffer.limit(buffer.limit() + (int) length);
        channel.read(buffer, start);
    }

    private static void readAFromMiddlePartition(ByteBuffer buffer, NavigableMap<Long, MessagePartition> middlePartitionMap, long aMin, long aMax) throws Exception {
        Set<Map.Entry<Long, MessagePartition>> entries = middlePartitionMap.entrySet();
        for (Map.Entry<Long, MessagePartition> entry : entries) {
            NavigableMap<Long, APartition> aPartitionMap = entry.getValue().aPartitionMap;

            long aStart=0;
            long aEnd=0;

            Map.Entry<Long, APartition> minFloorEntry = aPartitionMap.floorEntry(aMin);
            Map.Entry<Long, APartition> maxFloorEntry = aPartitionMap.floorEntry(aMax);

            if (minFloorEntry == null && maxFloorEntry == null) {
                return;
            } else if (minFloorEntry != null && maxFloorEntry != null) {
                aStart = minFloorEntry.getValue().aStart;
                aEnd = maxFloorEntry.getValue().aEnd;
            } else if (minFloorEntry != null) {
                aStart = minFloorEntry.getValue().aStart;
                aEnd = minFloorEntry.getValue().aEnd;
            } else if (maxFloorEntry != null) {
                aStart = maxFloorEntry.getValue().aStart;
                aEnd = maxFloorEntry.getValue().aEnd;
            }


            read(buffer, sortedAChannel, aStart, aEnd - aStart);

        }

    }

    public static MessageReadResult readAFast(long aMin, long aMax, long tMin, long tMax) throws Exception {
        ByteBuffer buffer = aLocalBuffer.get();
        buffer.position(0);
        buffer.limit(0);
        int mark = Integer.MAX_VALUE;
        NavigableMap<Long, MessagePartition> messagePartitionMap = PartitionIndex.getMessagePartitions(tMin, tMax);
        if (messagePartitionMap.size() <= 2) {
            MessagePartition firstPartition = messagePartitionMap.firstEntry().getValue();
            long aStart = PartitionIndex.getAStartInFirstPartition(firstPartition, tMin);
            MessagePartition lastPartition = messagePartitionMap.lastEntry().getValue();
            long aEnd = PartitionIndex.getAEndInLastPartition(lastPartition, tMax);
            read(buffer, aChannel, aStart, aEnd - aStart);
        } else {
            MessagePartition firstPartition = messagePartitionMap.firstEntry().getValue();
            long aFirstStart = PartitionIndex.getAStartInFirstPartition(firstPartition, tMin);
            long aFirstEnd = firstPartition.mEnd * 8;
            read(buffer, aChannel, aFirstStart, aFirstEnd - aFirstStart);

            MessagePartition lastPartition = messagePartitionMap.lastEntry().getValue();
            long aLastStart = lastPartition.mStart * 8;
            long aLastEnd = PartitionIndex.getAEndInLastPartition(lastPartition, tMax);
            read(buffer, aChannel, aLastStart, aLastEnd - aLastStart);
            mark = buffer.position();
            NavigableMap<Long, MessagePartition> middlePartitionMap = messagePartitionMap.subMap(messagePartitionMap.firstKey(), false, messagePartitionMap.lastKey(), false);
            readAFromMiddlePartition(buffer, middlePartitionMap, aMin, aMax);

        }

        buffer.flip();
        return new MessageReadResult(buffer, mark);
    }
//        int length = (int) (messageEnd - messageStart);
//        byteBuffer.clear();
//        byteBuffer.limit(length);
//
//        long breakpoint = -1;
//        long start = System.currentTimeMillis();
//        for (long i = min; i <= max; i++) {
//            ByteBuffer byteBuffer1 = MessageCache.map.get(i);
//            if (byteBuffer1 != null) {
//                for (int j = 0; j < byteBuffer1.limit(); j++) {
//                    byteBuffer.put(byteBuffer1.get(j));
//                }
//                length -= byteBuffer1.limit();
//            } else {
//                breakpoint = i;
//                break;
//            }
//        }
//        System.out.println("fill :" + (System.currentTimeMillis() - start));
//
//        if (breakpoint != -1) {
//            PartitionIndex.MessagePartition messagePartition = PartitionIndex.partitionMap.get(breakpoint);
//            if (messagePartition == null) {
//                byteBuffer.flip();
//                return byteBuffer;
//            }
//            messageStart = messagePartition.aStart;
//            adaptiveRead(byteBuffer, aChannel, messageStart);
//        } else {
//            byteBuffer.flip();
//        }
//        return byteBuffer;

//    }

    public static ByteBuffer readA(long tMin, long tMax) throws Exception {
        long aStart = PartitionIndex.getFirstMessagePartition(tMin).mStart * 8;
        long aEnd = PartitionIndex.getLastMessagePartition(tMax).mEnd * 8;
        ByteBuffer buffer = aLocalBuffer.get();
        buffer.position(0);
        buffer.limit(0);
        read(buffer, aChannel, aStart, aEnd - aStart);
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer readBody(long tMin, long tMax) throws Exception {
        long bodyStart = PartitionIndex.getFirstMessagePartition(tMin).mStart * Constants.Body_Size;
        long bodyEnd = PartitionIndex.getLastMessagePartition(tMax).mEnd * Constants.Body_Size;
        ByteBuffer buffer = bodyLocalBuffer.get();
        buffer.position(0);
        buffer.limit(0);
        read(buffer, bodyChannel, bodyStart, bodyEnd - bodyStart);
        buffer.flip();

        return buffer;
    }

    //private static Semaphore semaphore = new Semaphore(1);
    private static ByteBuffer adaptiveRead(ByteBuffer byteBuffer, FileChannel channel, long start) throws Exception {
//        if (length > 1024 * 1024) {
//            System.out.println("mmap:\t" + length);
//            return channel.aPartitionMap(FileChannel.MapMode.READ_ONLY, start, length);
//        } else {
//            semaphore.acquire();
        long readStart = System.currentTimeMillis();

        channel.read(byteBuffer, start);
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
