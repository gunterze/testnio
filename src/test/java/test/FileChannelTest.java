package test;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LongSummaryStatistics;
import java.util.Optional;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileChannelTest {
    static final Path PATH = Path.of(System.getProperty("ifile", "/tmp/ifile"));
    static final String HOST = System.getProperty("host", "localhost");
    static final int PORT = Integer.parseInt(System.getProperty("port", "6666"));
    static final Optional<Integer> SO_SNDBUF = Optional.ofNullable(System.getProperty("SO_SNDBUF"))
            .map(Integer::valueOf);
    static final Boolean TCP_NODELAY = Boolean.valueOf(Boolean.getBoolean("TCP_NODELAY"));

    private static Integer parseInteger(String value) {
        return value != null ? Integer.valueOf(value) : null;
    }


    @BeforeAll
    static void beforeAll() {
        testByteBuffer(ByteBufferSource.NEW_HEAP_8096, 1000);
        testByteBuffer(ByteBufferSource.NEW_DIRECT_8096, 1000);
        testByteArray(ByteArraySource.NEW_ARRAY_8096, 1000);
    }

    @AfterAll
    static void afterAll() throws IOException {
        long size = Files.size(PATH);
        float scale = 1000f / size;
        System.out.println("test, rmin, ravg, rmax, wmin, wavg, wmax");
        for (ByteArraySource value : ByteArraySource.values()) {
            if (value.readStat != null && value.writeStat != null)
                System.out.printf("%s [ms/GB], %f, %f, %f, %f, %f, %f%n", value,
                        value.readStat.getMin() * scale,
                        value.readStat.getAverage() * scale,
                        value.readStat.getMax() * scale,
                        value.writeStat.getMin() * scale,
                        value.writeStat.getAverage() * scale,
                        value.writeStat.getMax() * scale);
        }
        for (ByteBufferSource value : ByteBufferSource.values()) {
            if (value.readStat != null && value.writeStat != null)
                System.out.printf("%s [ms/GB], %f, %f, %f, %f, %f, %f%n", value,
                        value.readStat.getMin() * scale,
                        value.readStat.getAverage() * scale,
                        value.readStat.getMax() * scale,
                        value.writeStat.getMin() * scale,
                        value.writeStat.getAverage() * scale,
                        value.writeStat.getMax() * scale);
        }
    }

    @ParameterizedTest
    @EnumSource(ByteArraySource.class)
    void testByteArray(ByteArraySource action) {
        testByteArray(action, 100);
    }

    @ParameterizedTest
    @EnumSource(ByteBufferSource.class)
    void testByteBuffer(ByteBufferSource action) {
        testByteBuffer(action, 100);
    }

    static final byte[] b8096 = new byte[8096];
    static final byte[] b64768 = new byte[64768];
    enum ByteArraySource {
        SHARE_ARRAY_8096(() -> b8096),
        SHARE_ARRAY_64768(() -> b64768),
        NEW_ARRAY_8096(() -> new byte[8096]),
        NEW_ARRAY_64768(() -> new byte[64768]);

        final Supplier<byte[]> supplier;
        LongSummaryStatistics readStat;
        LongSummaryStatistics writeStat;

        ByteArraySource(Supplier<byte[]> supplier) {
            this.supplier = supplier;
        }
    }

    static ByteBuffer hbb8096 = ByteBuffer.allocate(8096);
    static ByteBuffer hbb64768 = ByteBuffer.allocate(64768);
    static ByteBuffer dbb8096 = ByteBuffer.allocateDirect(8096);
    static ByteBuffer dbb64768 = ByteBuffer.allocateDirect(64768);
    enum ByteBufferSource {
        SHARE_HEAP_8096(() -> hbb8096),
        SHARE_HEAP_64768(() -> hbb64768),
        SHARE_DIRECT_8096(() -> dbb8096),
        SHARE_DIRECT_64768(() -> dbb64768),
        NEW_HEAP_8096(() -> ByteBuffer.allocate(8096)),
        NEW_HEAP_64768(() -> ByteBuffer.allocate(64768)),
        NEW_DIRECT_8096(() -> ByteBuffer.allocateDirect(8096)),
        NEW_DIRECT_64768(() -> ByteBuffer.allocateDirect(64768));

        final Supplier<ByteBuffer> supplier;
        LongSummaryStatistics readStat;
        LongSummaryStatistics writeStat;

        ByteBufferSource(Supplier<ByteBuffer> supplier) {
            this.supplier = supplier;
        }
    }

    private static void testByteArray(ByteArraySource action, int n) {
        action.readStat = new LongSummaryStatistics();
        action.writeStat = new LongSummaryStatistics();
        try (Socket s = new Socket()) {
            if (SO_SNDBUF.isPresent())
                s.setOption(StandardSocketOptions.SO_SNDBUF, SO_SNDBUF.get());
            s.setOption(StandardSocketOptions.TCP_NODELAY, TCP_NODELAY);
            s.connect(new InetSocketAddress(HOST, PORT));
            OutputStream out = s.getOutputStream();
            for (int i = 0; i < n; i++) {
                testByteArray(out, action);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void testByteBuffer(ByteBufferSource action, int n) {
        action.readStat = new LongSummaryStatistics();
        action.writeStat = new LongSummaryStatistics();
        try (SocketChannel out = SocketChannel.open()) {
            if (SO_SNDBUF.isPresent())
                out.setOption(StandardSocketOptions.SO_SNDBUF, SO_SNDBUF.get());
            out.setOption(StandardSocketOptions.TCP_NODELAY, TCP_NODELAY);
            out.connect(new InetSocketAddress(HOST, PORT));
            for (int i = 0; i < n; i++) {
                testByteBuffer(out, action);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void testByteArray(OutputStream out, ByteArraySource action) {
        try (InputStream s = Files.newInputStream(PATH)) {
            int length = (int) Files.size(PATH);
            long readTime = 0L;
            long writeTime = 0L;
            long t2, t1 = System.nanoTime();
            do {
                byte[] b = action.supplier.get();
                if (length != 0) {
                    b[0] = (byte) length;
                    b[1] = (byte) (length >> 8);
                    b[2] = (byte) (length >> 16);
                    b[3] = (byte) (length >> 24);
                    length = b.length - 4;
                } else  {
                    length = b.length;
                }
                int read;
                while (length > 0 && (read = s.read(b, b.length - length, length)) > 0) {
                    length -= read;
                }
                t2 = System.nanoTime();
                readTime += t2 - t1;
                out.write(b, 0, b.length - length);
                t1 = System.nanoTime();
                writeTime += t1 - t2;
            } while (length == 0);
            action.readStat.accept(readTime);
            action.writeStat.accept(writeTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void testByteBuffer(SocketChannel out, ByteBufferSource action) {
        try (SeekableByteChannel s = Files.newByteChannel(PATH)) {
            ByteBuffer b;
            int length = (int) s.size();
            long readTime = 0L;
            long writeTime = 0L;
            long t2, t1 = System.nanoTime();
            int write = 0;
            do {
                b = action.supplier.get().clear();
                if (write == 0) {
                    b.put((byte) length);
                    b.put((byte) (length >> 8));
                    b.put((byte) (length >> 16));
                    b.put((byte) (length >> 24));
                }
                while (b.hasRemaining() && s.read(b) > 0);
                b.flip();
                t2 = System.nanoTime();
                readTime += t2 - t1;
                write += out.write(b);
                t1 = System.nanoTime();
                writeTime += t1 - t2;
            } while (b.position() == b.capacity());
            action.readStat.accept(readTime);
            action.writeStat.accept(writeTime);
            assertEquals(length + 4, write);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
