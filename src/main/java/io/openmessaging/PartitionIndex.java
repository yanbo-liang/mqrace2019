package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;

public class PartitionIndex {
    private static NavigableMap<Long, PartitionInfo> partitionMap = new TreeMap<>();
    private static long tMin = 0, tMax = 999;
    private static long startPosition = 0, totalByteIndexed = 0;

    public synchronized static void buildIndex(ByteBuffer buffer) {
        int i = 0;
        while (i < buffer.limit()) {
            long t = buffer.getLong(i);

            if (!(tMin <= t && t <= tMax)) {
                if (startPosition != totalByteIndexed) {
                    partitionMap.put(tMin / 1000, new PartitionInfo(startPosition, totalByteIndexed));
                    startPosition = totalByteIndexed;
                }
                tMin = (t / 1000) * 1000;
                tMax = tMin + 999;
            }

            totalByteIndexed += Constants.Message_Size;
            i += Constants.Message_Size;
        }
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


