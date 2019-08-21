package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;

public class PartitionIndex {
    private static NavigableMap<Long, PartitionInfo> partitionMap = new TreeMap<>();
    private static long partitionSize = 1000;
    private static long tMin = 0, tMax = partitionSize - 1;
    private static long startPosition = 0, totalByteIndexed = 0;
    private static int totalByteCompressed = 0;
    private static ByteBuffer tBuffer = ByteBuffer.allocate((int) partitionSize * 100 * 8);

    public synchronized static void flushIndex() {
        if (startPosition != totalByteIndexed) {
            tBuffer.flip();
            int byteCompressed = CompressUtil.compress(tBuffer, DirectBufferManager.getCompressedBuffer(), totalByteCompressed);
            tBuffer.clear();
            partitionMap.put(tMin / partitionSize, new PartitionInfo(startPosition, totalByteIndexed, totalByteCompressed));
            startPosition = totalByteIndexed;
            totalByteCompressed += byteCompressed;
        }
    }

    public synchronized static void buildIndex(ByteBuffer buffer) {
        int i = 0;
        while (i < buffer.limit()) {
            long t = buffer.getLong(i);
            if (!(tMin <= t && t <= tMax)) {
                flushIndex();
                tMin = (t / partitionSize) * partitionSize;
                tMax = tMin + partitionSize - 1;
            }
            tBuffer.putLong(t);
            totalByteIndexed += Constants.Message_Size;
            i += Constants.Message_Size;
        }
    }

    public static PartitionInfo firstPartitionInfo(long tMin) {
        return partitionMap.ceilingEntry(tMin / partitionSize).getValue();
    }

    public static PartitionInfo lastPartitionInfo(long tMax) {
        return partitionMap.floorEntry(tMax / partitionSize).getValue();
    }

    public static class PartitionInfo {
        long mStart, mEnd;
        int cStart;

        PartitionInfo(long mStart, long mEnd, int cStart) {
            this.mStart = mStart;
            this.mEnd = mEnd;
            this.cStart = cStart;
        }
    }
}


