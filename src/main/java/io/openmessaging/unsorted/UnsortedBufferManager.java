package io.openmessaging.unsorted;

import io.openmessaging.unsafe.UnsafeBuffer;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class UnsortedBufferManager {
    private static BlockingQueue<UnsafeBuffer> queue = new LinkedBlockingQueue<>();

    static {
        System.out.println(UnsortedConstants.Buffer_Queue_Size);
        for (long i = 0; i < UnsortedConstants.Buffer_Queue_Size; i++) {
            queue.offer(new UnsafeBuffer(UnsortedConstants.Buffer_Size));
        }
    }

    static UnsafeBuffer borrowBuffer() {
        try {
            UnsafeBuffer buffer = queue.take();
            buffer.clear();
            return buffer;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    static void returnBuffer(UnsafeBuffer buffer) {
        try {
            queue.put(buffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
