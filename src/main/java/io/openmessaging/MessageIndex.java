package io.openmessaging;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MessageIndex {
    private static NavigableMap<Long, Long> positionMap = new TreeMap<>();
    private static long totalByteIndexed = 0;

    public synchronized static void buildIndex(byte[] messageBuffer, int length) {

        for (int i = Constants.Index_Skip_Size; i < length; i += Constants.Index_Skip_Size) {

            long currentT = ByteUtils.getLong(messageBuffer, i);
            long previousT = ByteUtils.getLong(messageBuffer, i - Constants.Message_Size);

            while (i < length && currentT == previousT) {
                i += Constants.Message_Size;
                previousT = currentT;
                currentT = ByteUtils.getLong(messageBuffer, i);
            }
            positionMap.put(currentT, i + totalByteIndexed);
        }

        totalByteIndexed += length;
    }

    public synchronized static long readStartInclusive(long tMin) {

        Map.Entry<Long, Long> floorEntry = positionMap.floorEntry(tMin);
        if (floorEntry == null) {
            return 0;
        }
        System.out.println(tMin+" "+ floorEntry.getKey()+" "+ floorEntry.getValue());
        return floorEntry.getValue();
    }

    public synchronized static long readEndExclusive(long tMax) {
        Map.Entry<Long, Long> higherEntry = positionMap.higherEntry(tMax);
        if (higherEntry == null) {
            return totalByteIndexed;
        }
        System.out.println(tMax+" "+ higherEntry.getKey()+" "+ higherEntry.getValue());

        return higherEntry.getValue();
    }
}
