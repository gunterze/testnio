package test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

public class ServerSocketMain {
    static final Optional<Path> PATH = Optional.ofNullable(System.getProperty("ofile")).map(Path::of);
    static final int PORT = Integer.parseInt(System.getProperty("port", "6666"));
    static final Optional<Integer> SO_RCVBUF = Optional.ofNullable(System.getProperty("SO_RCVBUF"))
            .map(Integer::valueOf);
    static final byte[] b8096 = new byte[8096];
    static final byte[] b64768 = new byte[64768];
    enum ByteArraySource {
        SHARE_ARRAY_8096(() -> b8096),
        SHARE_ARRAY_64768(() -> b64768),
        NEW_ARRAY_8096(() -> new byte[8096]),
        NEW_ARRAY_64768(() -> new byte[64768]);

        final Supplier<byte[]> supplier;

        ByteArraySource(Supplier<byte[]> supplier) {
            this.supplier = supplier;
        }
    }

    public static void main(String[] args) {
        Supplier<byte[]> supplier = args.length > 0 ?  ByteArraySource.valueOf(args[0]).supplier : null;
        try (ServerSocket serverSocket = new ServerSocket()) {
            if (SO_RCVBUF.isPresent())
                serverSocket.setOption(StandardSocketOptions.SO_RCVBUF, SO_RCVBUF.get());
            serverSocket.bind(new InetSocketAddress(PORT));
            for (; ; ) {
                try (Socket socket = serverSocket.accept()) {
                    BufferedInputStream s = new BufferedInputStream(socket.getInputStream());
                    int b0, b1, b2, b3;
                    while (((b0 = s.read()) | (b1 = s.read()) | (b2 = s.read()) | (b3 = s.read())) >= 0) {
                        int length = (b0 & 255) | (b1 & 255) << 8 | (b2 & 255) << 16 | (b3 & 255) << 32;
                        if (PATH.isPresent()) {
                            try (OutputStream out = Files.newOutputStream(PATH.get())) {
                                while (length > 0) {
                                    byte[] b = supplier.get();
                                    int read;
                                    int pos = 0;
                                    int remaining = Math.min(b.length, length);
                                    while (remaining > 0 && (read = s.read(b, pos, remaining)) > 0) {
                                        remaining -= read;
                                        pos += read;
                                    }
                                    out.write(b, 0, pos);
                                    length -= pos;
                                }
                            }
                        } else {
                            s.skipNBytes(length);
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
