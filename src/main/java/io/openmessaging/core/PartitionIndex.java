package io.openmessaging.core;

import io.openmessaging.CompressUtil;
import io.openmessaging.Constants;
import io.openmessaging.DirectBufferManager;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.TreeMap;

public class PartitionIndex {
     static NavigableMap<Long, PartitionInfo> partitionMap = new TreeMap<>();
    private static long tMin = 0, tMax = Constants.Partition_Size - 1;
    private static long mStart = 0, mTotal = 0;
    private static long aStartPos = 0, aTotalByte = 0;
    private static int totalTCompressed = 0;
    private static ByteBuffer tBuffer = ByteBuffer.allocate((int) Constants.Partition_Size * 100 * 8);
    private static long top = (1L << 48) - 1;

    private static void compressLong(long a, ByteBuffer aBuffer) {
        if (a <= top) {
            aBuffer.put((byte) (a >> 40));
            aBuffer.put((byte) (a >> 32));
            aBuffer.put((byte) (a >> 24));
            aBuffer.put((byte) (a >> 16));
            aBuffer.put((byte) (a >> 8));
            aBuffer.put((byte) (a));
            aTotalByte += 6;
        } else {
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.putLong(a);
            aTotalByte += 14;
        }
    }

    public static void flushIndex() {
        if (mStart != mTotal) {
            tBuffer.flip();
            int byteCompressed = CompressUtil.compress(tBuffer, DirectBufferManager.getCompressedBuffer(), totalTCompressed);
            tBuffer.clear();
            partitionMap.put(tMin / Constants.Partition_Size, new PartitionInfo(mStart, mTotal, totalTCompressed, aStartPos, aTotalByte));
            mStart = mTotal;
            aStartPos = aTotalByte;
            totalTCompressed += byteCompressed;
        }
                System.out.println("totalTCompressed "+ totalTCompressed);

    }

    public static void buildIndex(long t, long a, ByteBuffer aBuffer) {
        if (!(tMin <= t && t <= tMax)) {
            flushIndex();
            tMin = (t / Constants.Partition_Size) * Constants.Partition_Size;
            tMax = tMin + Constants.Partition_Size - 1;
        }
        tBuffer.putLong(t);
        compressLong(a, aBuffer);
        mTotal += 1;
    }

    public static long getAStart(long tMin) {
        return partitionMap.ceilingEntry(tMin / Constants.Partition_Size).getValue().aStart;
    }

    public static long getAEnd(long tMax) {
        return partitionMap.floorEntry(tMax / Constants.Partition_Size).getValue().aEnd;
    }

    public static long getBodyStart(long tMin) {
        return partitionMap.ceilingEntry(tMin / Constants.Partition_Size).getValue().mStart * Constants.Body_Size;
    }

    public static long getBodyEnd(long tMax) {
        return partitionMap.floorEntry(tMax / Constants.Partition_Size).getValue().mEnd * Constants.Body_Size;
    }

//    public static long getAStart(long tMin) {
//        PartitionInfo partitionInfo = partitionMap.ceilingEntry(tMin / partitionSize).getValue();
//        long[] uncompressed = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), partitionInfo.cStart);
//        int i = 0;
//        for (; i < uncompressed.length; i++) {
//            if (uncompressed[i] >= tMin) {
//                break;
//            }
//        }
//        return (partitionInfo.mStart + i) * 8;
//    }
//
//    public static long getAEnd(long tMax) {
//        PartitionInfo partitionInfo = partitionMap.floorEntry(tMax / partitionSize).getValue();
//        long[] uncompressed = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), partitionInfo.cStart);
//        int i = 0;
//        for (; i < uncompressed.length; i++) {
//            if (uncompressed[uncompressed.length - 1 - i] <= tMax) {
//                break;
//            }
//        }
//        return (partitionInfo.mEnd - i) * 8;
//    }

    public static long[] getTArray(long tMin, long tMax) {
        NavigableMap<Long, PartitionInfo> subMap = partitionMap.subMap(tMin / Constants.Partition_Size, true, tMax / Constants.Partition_Size, true);
        int count = 0;
        for (PartitionInfo info : subMap.values()) {
            count += (info.mEnd - info.mStart);
        }
        long[] tArray = new long[count];
        int index = 0;
        for (PartitionInfo info : subMap.values()) {
            long[] uncompressed = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), info.cStart);
            System.arraycopy(uncompressed, 0, tArray, index, uncompressed.length);
            index += uncompressed.length;
        }
        return tArray;
    }

    public static class PartitionInfo {
        long mStart, mEnd;
        int cStart;
        long aStart, aEnd;

        PartitionInfo(long mStart, long mEnd, int cStart, long aStart, long aEnd) {
            this.mStart = mStart;
            this.mEnd = mEnd;
            this.cStart = cStart;
            this.aStart = aStart;
            this.aEnd = aEnd;
        }
    }
}


