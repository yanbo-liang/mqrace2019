package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class MessageWriterTask {
    private long[] messageBuffer;
    private int bufferLimit;
    private boolean end;

    public static MessageWriterTask createEndTask(long[] messageBuffer, int limit) {
        return new MessageWriterTask(messageBuffer, limit, true);
    }

    public MessageWriterTask(long[] messageBuffer, int limit) {

        this.messageBuffer = messageBuffer;
        this.bufferLimit=limit;
    }

    private MessageWriterTask(long[] messageBuffer, int bufferLimit, boolean end) {
        this.messageBuffer = messageBuffer;
        this.bufferLimit = bufferLimit;
        this.end = end;
    }

    public long[] getMessageBuffer() {
        return messageBuffer;
    }

    public int getBufferLimit() {
        return bufferLimit;
    }

    public boolean isEnd() {
        return end;
    }
}
