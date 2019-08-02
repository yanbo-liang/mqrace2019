package io.openmessaging;

import java.util.List;

public class WriterTask {
    Message[] messages;
    long start;
    long end;
boolean done;
    public WriterTask(Message[] messages, long start, long end) {
        this.messages = messages;
        this.start = start;
        this.end = end;
    }
}
