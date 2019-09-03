package io.openmessaging.core;

import java.nio.ByteBuffer;

public class MessageReadResult {
    public ByteBuffer buffer;
    public int mark;

    public MessageReadResult(ByteBuffer buffer, int mark) {
        this.buffer = buffer;
        this.mark = mark;
    }
}
