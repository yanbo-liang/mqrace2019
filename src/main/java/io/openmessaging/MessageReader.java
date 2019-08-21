package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageReader {
    private AsynchronousFileChannel messageChannel;
    private AsynchronousFileChannel headerChannel;

    public MessageReader() {
        try {
            messageChannel = AsynchronousFileChannel.open(Paths.get(Constants.Message_Path), StandardOpenOption.CREATE, StandardOpenOption.READ);
            headerChannel = AsynchronousFileChannel.open(Paths.get(Constants.Header_Path), StandardOpenOption.CREATE, StandardOpenOption.READ);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private class Callback implements CompletionHandler<Integer, ByteBuffer> {
        private AtomicInteger totalRead;

        public Callback() {

        }

        @Override
        public void completed(Integer result, ByteBuffer attachment) {

            synchronized (attachment) {
                System.out.println("byte read: " + result);
                attachment.notify();

            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer attachment) {
            exc.printStackTrace();
        }

    }

    public ByteBuffer read(long tMin, long tMax) {
        long s = System.currentTimeMillis();

        long start =PartitionIndex.firstPartitionInfo(tMin).mStart;
        long end = PartitionIndex.lastPartitionInfo(tMax).mEnd;
        System.out.println(start + " " + end);

        if (start >= end) {
            return null;
        }
        System.out.println("buildIndex:" + (System.currentTimeMillis() - s));

        ByteBuffer buffer = DirectBufferManager.borrowBuffer();
        buffer.limit((int) (end - start));
        long r = System.currentTimeMillis();

        synchronized (buffer) {
            messageChannel.read(buffer, start, buffer, new Callback());
            try {
                buffer.wait();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("read:" + (System.currentTimeMillis() - r));

        return buffer;
    }

    public void fastRead(ByteBuffer buffer, long tMin, long tMax) {
        PartitionIndex.PartitionInfo firstPartition = PartitionIndex.firstPartitionInfo(tMin);
        PartitionIndex.PartitionInfo lastPartition = PartitionIndex.lastPartitionInfo(tMax);

        long[] decompress = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), firstPartition.tStart);
        int i = 0;
        for (; i < decompress.length; i++) {
            if (decompress[i] >= tMin) {
                System.out.println(decompress[i] + " aa");
                break;
            }

        }
        decompress = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), lastPartition.tStart);
        int j = 0;
        for (; j < decompress.length; j++) {
            if (decompress[decompress.length - 1 - j] <= tMax) {
                System.out.println(decompress[decompress.length - 1 - j] + " bb");
                break;
            }

        }

        long start = (firstPartition.mStart / Constants.Message_Size + i) * 8;
        long end = (lastPartition.mEnd / Constants.Message_Size - j) * 8;

        buffer.limit((int) (end - start));
        System.out.println("i " + i + " j " + j);

        System.out.println("limit " + buffer.limit() + " start " + start);
        long r = System.currentTimeMillis();

        synchronized (buffer) {
            headerChannel.read(buffer, start, buffer, new Callback());
            try {
                buffer.wait();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("read:" + (System.currentTimeMillis() - r));
    }

}