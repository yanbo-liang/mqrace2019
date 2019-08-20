package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;

public class PartitionIndex {
    private static NavigableMap<Long, PartitionInfo> partitionMap = new TreeMap<>();
    private static long tMin = 0, tMax = 999;
    private static long startPosition = 0, totalByteIndexed = 0;
    private static int totalByteCompressed = 0;
    private static ByteBuffer tBuffer = ByteBuffer.allocate(100000 * 8);

    public synchronized static void buildIndex(ByteBuffer buffer) {
        int i = 0;
        while (i < buffer.limit()) {
            long t = buffer.getLong(i);

            if (!(tMin <= t && t <= tMax)) {

                if (startPosition != totalByteIndexed) {

                    tBuffer.flip();
                    int byteCompressed = CompressUtil.compress(tBuffer, DirectBufferManager.getCompressedBuffer(), totalByteCompressed);
                    tBuffer.clear();

                    partitionMap.put(tMin / 1000, new PartitionInfo(startPosition, totalByteIndexed, totalByteCompressed, totalByteCompressed + byteCompressed));
                    startPosition = totalByteIndexed;
                    totalByteCompressed += byteCompressed;


                }
                tMin = (t / 1000) * 1000;
                tMax = tMin + 999;
            }
            tBuffer.putLong(t);
            totalByteIndexed += Constants.Message_Size;
            i += Constants.Message_Size;
        }
        System.out.println(totalByteCompressed);
    }

    public synchronized static void completeIndex() {
        if (startPosition != totalByteIndexed) {
            tBuffer.flip();
            int byteCompressed = CompressUtil.compress(tBuffer, DirectBufferManager.getCompressedBuffer(), totalByteCompressed);
            tBuffer.clear();

            partitionMap.put(tMin / 1000, new PartitionInfo(startPosition, totalByteIndexed, totalByteCompressed, totalByteCompressed + byteCompressed));
            startPosition = totalByteIndexed;
            totalByteCompressed += byteCompressed;
        }
    }

    public synchronized static long a(long tMin) {
        System.out.println("map Size " + partitionMap.size());
        return partitionMap.ceilingEntry(tMin / 1000).getValue().mStart;
    }

    public synchronized static long b(long tMax) {
        return partitionMap.floorEntry(tMax / 1000).getValue().mEnd;

    }

    public synchronized static NavigableMap<Long, PartitionInfo> bc(long tMin, long tMax) {
        return partitionMap.subMap(tMin / 1000, true, tMax / 1000, true);
    }

    public static class PartitionInfo {
        long mStart, mEnd;
        int tStart, tEnd;

        PartitionInfo(long mStart, long mEnd, int tStart, int tEnd) {
            this.mStart = mStart;
            this.mEnd = mEnd;
            this.tStart = tStart;
            this.tEnd = tEnd;

        }
    }
}


