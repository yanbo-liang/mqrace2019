package io.openmessaging.unsafe;

public class UnsafeWriterTask {
    UnsafeBuffer unsafeBuffer;
    boolean isEnd;

    UnsafeWriterTask(UnsafeBuffer unsafeBuffer, boolean isEnd) {
        this.unsafeBuffer = unsafeBuffer;
        this.isEnd = isEnd;
    }
}