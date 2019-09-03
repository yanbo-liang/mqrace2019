package io.openmessaging.core;

import io.openmessaging.CompressUtil;
import io.openmessaging.Constants;
import io.openmessaging.DirectBufferManager;
import io.openmessaging.core.partition.APartition;
import io.openmessaging.core.partition.MessagePartition;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class PartitionIndex {
    static NavigableMap<Long, MessagePartition> partitionMap = new TreeMap<>();
    private static long tMin = 0, tMax = Constants.Partition_Size - 1;
    private static long mStart = 0, mTotal = 0;
    private static int totalTCompressed = 0;
    private static ByteBuffer tBuffer = ByteBuffer.allocate((int) Constants.Partition_Size * 100 * 8);
    private static ThreadLocal<LongBuffer> localLongBuffer = ThreadLocal.withInitial(() -> LongBuffer.allocate(500000));


    private static long aStart = 0, aTotal = 0;
    private static long aPartitionCount = 10;
    private static long aPartitionSize = Constants.A_Mark / 10;
    private static long aMin = 0, aMax = aPartitionSize - 1;
    private static NavigableMap<Long, APartition> aPartitionMap = new TreeMap<>();


    public static void flushIndex() {
        flushAIndex();
        aPartitionCount = 10;
        aMin = 0;
        aMax = aPartitionSize - 1;
        if (mStart != mTotal) {
            tBuffer.flip();
            int byteCompressed = CompressUtil.compress(tBuffer, DirectBufferManager.getCompressedBuffer(), totalTCompressed);
            tBuffer.clear();
            partitionMap.put(tMin, new MessagePartition(mStart, mTotal, totalTCompressed, aPartitionMap));
            aPartitionMap.clear();
            mStart = mTotal;
            totalTCompressed += byteCompressed;
        }
//        System.out.println("totalTCompressed " + totalTCompressed);
    }

    public static void flushAIndex() {
        if (aStart != aTotal) {
            aPartitionMap.put(aMin, new APartition(aStart, aTotal));
            aStart = aTotal;
        }
    }

    public static void buildIndex(long t, long a, ByteBuffer aBuffer) {
        if (!(tMin <= t && t <= tMax)) {
            flushIndex();
            tMin = (t / Constants.Partition_Size) * Constants.Partition_Size;
            tMax = tMin + Constants.Partition_Size - 1;
        }
        tBuffer.putLong(t);
        mTotal += 1;

        while (!(aMin <= a && a <= aMax)) {
            flushAIndex();
            if (aPartitionCount == 0) {
                aMin = Constants.A_Mark / 10 * 10;
                aMax = Long.MAX_VALUE;
                break;
            }
            aMin += aPartitionSize;
            aMax += aPartitionSize;
            aPartitionCount -= 1;
        }

        aTotal += CompressUtil.compressLong(a, aBuffer);
    }

    public static MessagePartition getFirstMessagePartition(long tMin) {
        return partitionMap.ceilingEntry(tMin / Constants.Partition_Size * Constants.Partition_Size).getValue();
    }

    public static MessagePartition getLastMessagePartition(long tMax) {
        return partitionMap.floorEntry(tMax).getValue();
    }

    public static NavigableMap<Long, MessagePartition> getMessagePartitions(long tMin, long tMax) {
        return partitionMap.subMap(tMin / Constants.Partition_Size * Constants.Partition_Size, true, tMax, true);
    }

    public static long getAStartInFirstPartition(MessagePartition firstPartition, long tMin) {
        long[] uncompressed = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), firstPartition.cStart);
        int i = 0;
        for (; i < uncompressed.length; i++) {
            if (uncompressed[i] >= tMin) {
                break;
            }
        }
        return (firstPartition.mStart + i) * 8;
    }

    public static long getAEndInLastPartition(MessagePartition lastPartition, long tMax) {
        long[] uncompressed = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), lastPartition.cStart);
        int i = 0;
        for (; i < uncompressed.length; i++) {
            if (uncompressed[uncompressed.length - 1 - i] <= tMax) {
                break;
            }
        }
        return (lastPartition.mEnd - i) * 8;
    }

    public static LongBuffer getTArray(long tMin, long tMax) {
        NavigableMap<Long, MessagePartition> subMap = getMessagePartitions(tMin, tMax);

        LongBuffer longBuffer = localLongBuffer.get();
        longBuffer.clear();
        for (MessagePartition info : subMap.values()) {
            CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), longBuffer, info.cStart);
        }
        longBuffer.flip();
        return longBuffer;
    }


}


