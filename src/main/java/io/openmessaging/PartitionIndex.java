package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;

public class PartitionIndex {
    private static NavigableMap<Long, PartitionInfo> partitionMap = new TreeMap<>();
    private static long tMin = 0, tMax = 1999;
    private static long aMin = Long.MAX_VALUE, aMax = Long.MIN_VALUE;
    private static long startPosition = 0, totalByteIndexed = 0;
    private static long sum = 0;
    private static int count = 0;


    private static void findRangeForT(long t) {
        tMin = (t / 2000) * 2000;
        tMax = tMin + 1999;
    }

    public synchronized static void index(ByteBuffer buffer) {
        int i = 0;
        while (i < buffer.limit()) {
            long t = buffer.getLong(i);
            long a = buffer.getLong(i + 8);

            if (tMin <= t && t <= tMax) {
                if (a < aMin) {
                    aMin = a;
                }
                if (a > aMax) {
                    aMax = a;
                }
                count += 1;
                sum += a;
                totalByteIndexed += Constants.Message_Size;
            } else {
                if (aMin != Integer.MAX_VALUE) {
                    partitionMap.put(tMin / 2000, new PartitionInfo(aMin, aMax, startPosition, totalByteIndexed, count, sum));
                    startPosition = totalByteIndexed;
                }
                findRangeForT(t);
                aMin = Long.MAX_VALUE;
                aMax = Long.MIN_VALUE;
                count = 0;
                sum = 0;

                if (a < aMin) {
                    aMin = a;
                }
                if (a > aMax) {
                    aMax = a;
                }
                count += 1;
                sum += a;
                totalByteIndexed += Constants.Message_Size;
            }
            i += Constants.Message_Size;
        }
    }

    public synchronized static void complete() {
        partitionMap.put(tMin / 2000, new PartitionInfo(aMin, aMax, startPosition, totalByteIndexed, count, sum));
    }

    public synchronized static long a(long tMin) {
        System.out.println("map Size "+partitionMap.size());
        return partitionMap.ceilingEntry(tMin / 2000).getValue().start;
    }

    public synchronized static long b(long tMax) {
        return partitionMap.floorEntry(tMax / 2000).getValue().end;

    }

    public synchronized static NavigableMap<Long, PartitionInfo> bc( long tMin, long tMax) {
        return partitionMap.subMap(tMin / 2000, true, tMax / 2000, true);
    }

    public static class PartitionInfo {
        long aMin, aMax;
        long start, end;
        int count;
        long sum;

        PartitionInfo(long aMin, long aMax, long start, long end, int count, long sum) {
            this.aMin = aMin;
            this.aMax = aMax;
            this.start = start;
            this.end = end;
            this.count = count;
            this.sum = sum;
        }
    }
}


