package io.openmessaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageReader {
    private AsynchronousFileChannel fileChannel;
    private AsynchronousFileChannel fileChannelNoData;

    public MessageReader() {
        try {
            Path path = Paths.get(Constants.Path);
            fileChannel = AsynchronousFileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ);
            Path nodata = Paths.get(Constants.Header_Path);

            fileChannelNoData = AsynchronousFileChannel.open(nodata, StandardOpenOption.CREATE, StandardOpenOption.READ);

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
//        long start = MessageIndex.readStartInclusive(tMin);
////        long end = MessageIndex.readEndExclusive(tMax);

        long start = PartitionIndex.a((int) tMin);
        long end = PartitionIndex.b((int) tMax);
        if (start == -1 || end == -1) {
            return null;
        }
        if (start >= end) {
            return null;
        }
        System.out.println("index:" + (System.currentTimeMillis() - s));

        ByteBuffer buffer = DirectBufferManager.borrowBuffer();
        buffer.limit((int) (end - start));
        long r = System.currentTimeMillis();

        synchronized (buffer) {
            fileChannel.read(buffer, start, buffer, new Callback());
            try {
                buffer.wait();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("read:" + (System.currentTimeMillis() - r));

        return buffer;
    }

    public ByteBuffer readMissedPartition(List<PartitionIndex.PartitionInfo> partitionInfoList) {
        ByteBuffer buffer = DirectBufferManager.borrowBuffer();
buffer.limit(0);
        Callback callback = new Callback();
        for (int i = 0; i < partitionInfoList.size(); i++) {
            PartitionIndex.PartitionInfo partitionInfo = partitionInfoList.get(i);
            buffer.limit((buffer.limit() + (int) (partitionInfo.end - partitionInfo.start)));
            synchronized (buffer) {

                fileChannel.read(buffer, partitionInfo.start, buffer, callback);
                try {
                    buffer.wait();

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }

        return buffer;
    }
}