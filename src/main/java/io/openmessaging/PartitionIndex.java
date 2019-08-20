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
    private static ByteBuffer tCompressed = ByteBuffer.allocate(1024 * 1024 * 1024);

    public synchronized static void buildIndex(ByteBuffer buffer) {
        int i = 0;
        while (i < buffer.limit()) {
            long t = buffer.getLong(i);

            if (!(tMin <= t && t <= tMax)) {
                if (startPosition != totalByteIndexed) {
                    partitionMap.put(tMin / 1000, new PartitionInfo(startPosition, totalByteIndexed));
                    startPosition = totalByteIndexed;

                    int byteCompressed = CompressUtil.compress(tBuffer, tCompressed, totalByteCompressed);
                    totalByteCompressed += byteCompressed;
                    tBuffer.clear();
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
            partitionMap.put(tMin / 1000, new PartitionInfo(startPosition, totalByteIndexed));
        }
    }

    public synchronized static long a(long tMin) {
        System.out.println("map Size " + partitionMap.size());
        return partitionMap.ceilingEntry(tMin / 1000).getValue().start;
    }

    public synchronized static long b(long tMax) {
        return partitionMap.floorEntry(tMax / 1000).getValue().end;

    }

    public synchronized static NavigableMap<Long, PartitionInfo> bc(long tMin, long tMax) {
        return partitionMap.subMap(tMin / 1000, true, tMax / 1000, true);
    }

    public static class PartitionInfo {
        long start, end;

        PartitionInfo(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }
}


