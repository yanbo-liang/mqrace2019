package io.openmessaging;

public class MessageWriterTask {
    private byte[] messageBuffer;
    private int bufferLimit;
    private boolean end;

    public static MessageWriterTask createEndTask(byte[] messageBuffer, int limit) {
        return new MessageWriterTask(messageBuffer, limit, true);
    }

    public MessageWriterTask(byte[] messageBuffer) {
        this.messageBuffer = messageBuffer;
    }

    private MessageWriterTask(byte[] messageBuffer, int bufferLimit, boolean end) {
        this.messageBuffer = messageBuffer;
        this.bufferLimit = bufferLimit;
        this.end = end;
    }

    public byte[] getMessageBuffer() {
        return messageBuffer;
    }

    public int getBufferLimit() {
        return bufferLimit;
    }

    public boolean isEnd() {
        return end;
    }
}
