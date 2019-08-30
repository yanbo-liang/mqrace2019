package io.openmessaging;

class MessageBatchWrapper {
    long[] tArray;
    long[] aArray;
    byte[] bodyArray;
    boolean isEnd;
    int size;

    MessageBatchWrapper(int batchSize, boolean isEnd) {
        tArray = new long[batchSize];
        aArray = new long[batchSize];
        bodyArray = new byte[batchSize * Constants.Body_Size];
        this.isEnd = isEnd;
    }

    static void copy(MessageBatchWrapper src, int srcStart, MessageBatchWrapper dest, int destStart, int length) {
        System.arraycopy(src.tArray, srcStart, dest.tArray, destStart, length);
        System.arraycopy(src.aArray, srcStart, dest.aArray, destStart, length);
        System.arraycopy(src.bodyArray, srcStart*Constants.Body_Size, dest.bodyArray, destStart*Constants.Body_Size, length * Constants.Body_Size);
    }
}
