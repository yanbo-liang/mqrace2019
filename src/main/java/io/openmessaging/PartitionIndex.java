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

    public static long getMessageStart(long tMin) {
        return partitionMap.ceilingEntry(tMin / partitionSize).getValue().mStart;
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


