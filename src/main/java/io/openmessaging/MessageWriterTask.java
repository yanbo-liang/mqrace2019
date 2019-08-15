package io.openmessaging;

import java.nio.ByteBuffer;

public class MessageWriterTask {
    private ByteBuffer messageBuffer;
    private int bufferLimit;
    private boolean end;

    public static MessageWriterTask createEndTask(ByteBuffer messageBuffer, int limit) {
        return new MessageWriterTask(messageBuffer, limit, true);
    }

    public MessageWriterTask(ByteBuffer messageBuffer) {
        this.messageBuffer = messageBuffer;
    }

    private MessageWriterTask(ByteBuffer messageBuffer, int bufferLimit, boolean end) {
        this.messageBuffer = messageBuffer;
        this.bufferLimit = bufferLimit;
        this.end = end;
    }

    public ByteBuffer getMessageBuffer() {
        return messageBuffer;
    }

    public int getBufferLimit() {
        return bufferLimit;
    }

    public boolean isEnd() {
        return end;
    }
}
