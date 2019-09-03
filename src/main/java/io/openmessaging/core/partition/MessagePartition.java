package io.openmessaging.core.partition;

import java.util.NavigableMap;
import java.util.TreeMap;

public class MessagePartition {
    public long mStart, mEnd;
    public int cStart;
    public NavigableMap<Long, APartition> aPartitionMap = new TreeMap<>();

    public MessagePartition(long mStart, long mEnd, int cStart, NavigableMap<Long, APartition> srcMap) {
        this.mStart = mStart;
        this.mEnd = mEnd;
        this.cStart = cStart;
        this.aPartitionMap.putAll(srcMap);
    }
}