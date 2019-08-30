package io.openmessaging.core;

import io.openmessaging.Constants;
import io.openmessaging.Message;

import java.nio.ByteBuffer;

class MessageBatchWrapper {
    long[] tArray;
    ByteBuffer aArray;
    byte[] bodyArray;
    boolean isEnd = false;
    int size = 0;

    MessageBatchWrapper(int batchSize) {
        tArray = new long[batchSize];
        aArray = ByteBuffer.allocate(batchSize * 8);
        bodyArray = new byte[batchSize * Constants.Body_Size];
    }

    void putMessage(int index, Message message) {
        tArray[index] = message.getT();
//        aArray[index] = message.getA();
        aArray.putLong(index * 8, message.getA());
        System.arraycopy(message.getBody(), 0, bodyArray, index * Constants.Body_Size, Constants.Body_Size);
    }

    static void copy(MessageBatchWrapper src, int srcStart, MessageBatchWrapper dest, int destStart, int length) {
        System.arraycopy(src.tArray, srcStart, dest.tArray, destStart, length);
//        System.arraycopy(src.aArray, srcStart, dest.aArray, destStart, length);
        System.arraycopy(src.aArray.array(), srcStart * 8, dest.aArray.array(), destStart * 8, length * 8);
        System.arraycopy(src.bodyArray, srcStart * Constants.Body_Size, dest.bodyArray, destStart * Constants.Body_Size, length * Constants.Body_Size);
    }
}
