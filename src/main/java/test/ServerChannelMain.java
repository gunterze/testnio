package test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

public class ServerChannelMain {
    static final Optional<Path> PATH = Optional.ofNullable(System.getProperty("ofile")).map(Path::of);
    static final int PORT = Integer.parseInt(System.getProperty("port", "6666"));
    static final Optional<Integer> SO_RCVBUF = Optional.ofNullable(System.getProperty("SO_RCVBUF"))
            .map(Integer::valueOf);
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
        ByteBufferSource(Supplier<ByteBuffer> supplier) {
            this.supplier = supplier;
        }
    }

    public static void main(String[] args) {
        Supplier<ByteBuffer> supplier =
                (args.length > 0 ? ByteBufferSource.valueOf(args[0]) : ByteBufferSource.SHARE_HEAP_64768).supplier;
        try (ServerSocketChannel server = ServerSocketChannel.open()) {
            if (SO_RCVBUF.isPresent())
                server.setOption(StandardSocketOptions.SO_RCVBUF, SO_RCVBUF.get());
            server.bind(new InetSocketAddress(PORT));
            for (; ; ) {
                try (SocketChannel s = server.accept()) {
                    ByteBuffer buf = supplier.get();
                    buf.clear();
                    buf.limit(4);
                    while (s.read(buf) > 0) {
                        int length = (buf.get(0) & 255)
                                | (buf.get(1) & 255) << 8
                                | (buf.get(2) & 255) << 16
                                | (buf.get(3) & 255) << 32;
                        if (PATH.isPresent()) {
                            try (SeekableByteChannel out = Files.newByteChannel(PATH.get())) {
                                transfer(s, length, out, buf, supplier);
                            }
                        } else {
                            transfer(s, length, null, buf, supplier);
                        }
                        buf = supplier.get();
                        buf.clear();
                        buf.limit(4);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void transfer(SocketChannel src, int length, SeekableByteChannel dst, ByteBuffer buf,
                                 Supplier<ByteBuffer> supplier)
            throws IOException {
        for (;;) {
            buf.clear();
            if (length < buf.capacity())
                buf.limit(length);
            while (buf.hasRemaining() && src.read(buf) > 0);
            length -= buf.position();
            buf.flip();
            if (dst != null) dst.write(buf);
            if (length == 0) break;
            buf = supplier.get();
        }
    }
}
