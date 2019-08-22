package io.openmessaging;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class MessageWriterTask {
    private LongBuffer messageBuffer;
    private int bufferLimit;
    private boolean end;

    public static MessageWriterTask createEndTask(LongBuffer messageBuffer, int limit) {
        return new MessageWriterTask(messageBuffer, limit, true);
    }

    public MessageWriterTask(LongBuffer messageBuffer) {
        this.messageBuffer = messageBuffer;
    }

    private MessageWriterTask(LongBuffer messageBuffer, int bufferLimit, boolean end) {
        this.messageBuffer = messageBuffer;
        this.bufferLimit = bufferLimit;
        this.end = end;
    }

    public LongBuffer getMessageBuffer() {
        return messageBuffer;
    }

    public int getBufferLimit() {
        return bufferLimit;
    }

    public boolean isEnd() {
        return end;
    }
}
