package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class PartitionIndex {
    private static NavigableMap<Integer, PartitionInfo> partitionMap = new TreeMap<>();
    private static int min = 0, max = 999;
    private static int aMin = Integer.MAX_VALUE, aMax = Integer.MIN_VALUE;
    private static long startPosition = 0, totalByteIndexed = 0;

    private static void findRangeForT(int t) {
        while (!(min <= t && t <= max)) {
            min += 1000;
            max += 1000;
        }
    }

    public synchronized static void index(ByteBuffer buffer) {
        int i = 0;
        while (i < buffer.limit()) {
            int t = (int)buffer.getLong(i);
            int a = (int)buffer.getLong(i + 8);
            if (min <= t && t <= max) {
                if (a < aMin) {
                    aMin = a;
                }
                if (a > aMax) {
                    aMax = a;
                }
                totalByteIndexed += Constants.Message_Size;
            } else {
                if (aMin != Integer.MAX_VALUE) {
                    partitionMap.put(min / 1000, new PartitionInfo(aMin, aMax, startPosition, totalByteIndexed));
                    startPosition = totalByteIndexed;
                }
                findRangeForT(t);
                aMin = Integer.MAX_VALUE;
                aMax = Integer.MIN_VALUE;
                if (a < aMin) {
                    aMin = a;
                }
                if (a > aMax) {
                    aMax = a;
                }
                totalByteIndexed += Constants.Message_Size;
            }
            i += Constants.Message_Size;
        }
    }

    public synchronized static void complete() {
        partitionMap.put(min / 1000, new PartitionInfo(aMin,aMax, startPosition, totalByteIndexed));
    }

    public synchronized static long a(int tMin) {
        int startPartition = tMin / 1000;
        return partitionMap.ceilingEntry(startPartition).getValue().start;
 }

    public synchronized static long b(int tMax) {
        int endPartition = tMax / 1000;
         return partitionMap.floorEntry(endPartition).getValue().end;

    }

    static class PartitionInfo {
        int low;
        int high;
        long start;
        long end;

        PartitionInfo(int low, int high, long start, long end) {
            this.low = low;
            this.high = high;
            this.start = start;
            this.end = end;
        }
    }
}


