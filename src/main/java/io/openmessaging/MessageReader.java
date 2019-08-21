package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.Channel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

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

    public void read(ByteBuffer buffer, long tMin, long tMax) throws InterruptedException {
        long start = PartitionIndex.firstPartitionInfo(tMin).mStart;
        long end = PartitionIndex.lastPartitionInfo(tMax).mEnd;
        asyncRead(buffer, messageChannel, start, end - start);
    }

    public void fastRead(ByteBuffer buffer, long tMin, long tMax) throws InterruptedException {
        PartitionIndex.PartitionInfo firstPartition = PartitionIndex.firstPartitionInfo(tMin);
        PartitionIndex.PartitionInfo lastPartition = PartitionIndex.lastPartitionInfo(tMax);

        long[] uncompressedT = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), firstPartition.cStart);
        int i = 0;
        for (; i < uncompressedT.length; i++) {
            if (uncompressedT[i] >= tMin) {
                System.out.println(uncompressedT[i] + " aa");
                break;
            }

        }
        uncompressedT = CompressUtil.decompress(DirectBufferManager.getCompressedBuffer(), lastPartition.cStart);
        int j = 0;
        for (; j < uncompressedT.length; j++) {
            if (uncompressedT[uncompressedT.length - 1 - j] <= tMax) {
                System.out.println(uncompressedT[uncompressedT.length - 1 - j] + " bb");
                break;
            }
        }

        long start = (firstPartition.mStart / Constants.Message_Size + i) * 8;
        long end = (lastPartition.mEnd / Constants.Message_Size - j) * 8;

        System.out.println("mStart " + firstPartition.mStart + " mEnd " + lastPartition.mEnd);
        System.out.println("i " + i + " j " + j);

        System.out.println("limit " + buffer.limit() + " start " + start);
        buffer.limit((int) (end - start));

        asyncRead(buffer, headerChannel, start, end - start);
    }

    private void asyncRead(ByteBuffer buffer, AsynchronousFileChannel channel, long start, long length) throws InterruptedException {
        long readStart = System.currentTimeMillis();
        synchronized (buffer) {
            buffer.limit((int) length);
            channel.read(buffer, start, buffer, new WriteCompletionHandler());
            buffer.wait();
        }
        System.out.println("read:" + (System.currentTimeMillis() - readStart));
    }

    private class WriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer buffer) {
            synchronized (buffer) {
                System.out.println("byte read: " + result);
                buffer.notify();
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer attachment) {
            exc.printStackTrace();
        }
    }
}