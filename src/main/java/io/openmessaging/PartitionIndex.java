package io.openmessaging;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class PartitionIndex {
    private static NavigableMap<Long, PartitionInfo> partitionMap = new TreeMap<>();
    private static long partitionSize = 1000;
    private static long tMin = 0, tMax = partitionSize - 1;
    private static long startPosition = 0, totalByteIndexed = 0;
    private static int totalTCompressed = 0;
    static long aStartPoition = 0, totalACompressed = 0;
    private static ByteBuffer tBuffer = ByteBuffer.allocate((int) partitionSize * 100 * 8);
    private static long aCap = (1L << 48) - 1;

    private static void compressLong(long a, ByteBuffer aBuffer) {
        if (a <= aCap) {
            aBuffer.put((byte) (a >> 40));
            aBuffer.put((byte) (a >> 32));
            aBuffer.put((byte) (a >> 24));
            aBuffer.put((byte) (a >> 16));
            aBuffer.put((byte) (a >> 8));
            aBuffer.put((byte) (a));
            totalACompressed += 6;
        } else {
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.put((byte) 0);
            aBuffer.putLong(a);
            totalACompressed += 14;
        }
    }

    public static void flushIndex() {
        if (startPosition != totalByteIndexed) {
            tBuffer.flip();
            int byteCompressed = CompressUtil.compress(tBuffer, DirectBufferManager.getCompressedBuffer(), totalTCompressed);
            tBuffer.clear();
            partitionMap.put(tMin / partitionSize, new PartitionInfo(startPosition, totalByteIndexed, totalTCompressed, aStartPoition, totalACompressed));
            startPosition = totalByteIndexed;
            aStartPoition = totalACompressed;
            totalTCompressed += byteCompressed;
        }
    }

    public static void buildIndex(long t, long a, ByteBuffer aBuffer) {
        if (!(tMin <= t && t <= tMax)) {
            flushIndex();
            tMin = (t / partitionSize) * partitionSize;
            tMax = tMin + partitionSize - 1;
        }
        tBuffer.putLong(t);
        compressLong(a, aBuffer);
        totalByteIndexed += Constants.Message_Size;
//        System.out.println("totalTCompressed "+ totalTCompressed);
    }

    public static long getMessageStart(long tMin) {
        Map.Entry<Long, PartitionInfo> longPartitionInfoEntry = partitionMap.ceilingEntry(tMin / partitionSize);
        if (longPartitionInfoEntry == null) {
            System.out.println();
        }
        return longPartitionInfoEntry.getValue().mStart;
    }

    public static long getMessageEnd(long tMax) {
        return partitionMap.floorEntry(tMax / partitionSize).getValue().mEnd;
    }

    public static long getAStart(long tMin) {
        PartitionInfo partitionInfo = partitionMap.ceilingEntry(tMin / partitionSize).getValue();
        long[] uncompressed = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), partitionInfo.cStart);
        int i = 0;
        for (; i < uncompressed.length; i++) {
            if (uncompressed[i] >= tMin) {
                break;
            }
        }
        return (partitionInfo.mStart / Constants.Message_Size + i) * 8;
    }

    public static long getAEnd(long tMax) {
        PartitionInfo partitionInfo = partitionMap.floorEntry(tMax / partitionSize).getValue();
        long[] uncompressed = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), partitionInfo.cStart);
        int i = 0;
        for (; i < uncompressed.length; i++) {
            if (uncompressed[uncompressed.length - 1 - i] <= tMax) {
                break;
            }
        }
        return (partitionInfo.mEnd / Constants.Message_Size - i) * 8;
    }

    public static long[] getTArray(long tMin, long tMax) {
        NavigableMap<Long, PartitionInfo> subMap = partitionMap.subMap(tMin / partitionSize, true, tMax / partitionSize, true);
        int count = 0;
        for (PartitionInfo info : subMap.values()) {
            count += (info.mEnd - info.mStart) / Constants.Message_Size;
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


