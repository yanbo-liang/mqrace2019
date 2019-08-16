package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;

public class PartitionIndex {
    private static NavigableMap<Integer, PartitionInfo> partitionMap = new TreeMap<>();
    private static int min = 0, max = 1999;
    private static int aMin = Integer.MAX_VALUE, aMax = Integer.MIN_VALUE;
    private static long startPosition = 0, totalByteIndexed = 0;
    private static int count = 0;
    private static long sum = 0;

    private static ByteBuffer tmp = ByteBuffer.allocate(2000 * 10 * 4);
    private static ByteBuffer compressed = ByteBuffer.allocate(1024 * 1024 * 1024);

    private static void findRangeForT(int t) {
        while (!(min <= t && t <= max)) {
            min += 2000;
            max += 2000;
        }
    }
    static int compressd=0;
    public synchronized static void index(ByteBuffer buffer) {
        int i = 0;
        while (i < buffer.limit()) {
            int t = (int) buffer.getLong(i);
            int a = (int) buffer.getLong(i + 8);
//            tmp.putInt(a-t);
                        tmp.putInt(t);

            if (min <= t && t <= max) {
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
                    partitionMap.put(min / 2000, new PartitionInfo(aMin, aMax, startPosition, totalByteIndexed, count, sum));
                    startPosition = totalByteIndexed;
                    compressd +=  CompressUtil.compress(tmp, compressed);
                }
                findRangeForT(t);
                aMin = Integer.MAX_VALUE;
                aMax = Integer.MIN_VALUE;
                count = 0;
                sum = 0;
                tmp.clear();
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
        partitionMap.put(min / 2000, new PartitionInfo(aMin, aMax, startPosition, totalByteIndexed, count, sum));
        System.out.println("total Compressed "+compressd);
    }

    public synchronized static long a(long tMin) {
        int startPartition = (int) tMin / 2000;
        return partitionMap.ceilingEntry(startPartition).getValue().start;
    }

    public synchronized static long b(long tMax) {
        int endPartition = (int) tMax / 2000;
        return partitionMap.floorEntry(endPartition).getValue().end;

    }

    public synchronized static NavigableMap<Integer, PartitionInfo> bc(long aMin, long aMax, long tMin, long tMax) {
        return partitionMap.subMap((int) tMin / 2000, true, (int) tMax / 2000, true);
    }

    public static class PartitionInfo {
        int low;
        int high;
        long start;
        long end;
        int count;
        long sum;

        PartitionInfo(int low, int high, long start, long end, int count, long sum) {
            this.low = low;
            this.high = high;
            this.start = start;
            this.end = end;
            this.count = count;
            this.sum = sum;
        }
    }
}


