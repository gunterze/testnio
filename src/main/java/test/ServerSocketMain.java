package test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

public class ServerSocketMain {
    static final Optional<Path> PATH = Optional.ofNullable(System.getProperty("ofile")).map(Path::of);
    static final int PORT = Integer.parseInt(System.getProperty("port", "6666"));
    static final Optional<Integer> SO_RCVBUF = Optional.ofNullable(System.getProperty("SO_RCVBUF"))
            .map(Integer::valueOf);
    static final boolean NO_BUFSTREAM = Boolean.getBoolean("NO_BUFSTREAM");
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
        byte[] b4 = new byte[4];
        Supplier<byte[]> supplier = args.length > 0 ?  ByteArraySource.valueOf(args[0]).supplier : null;
        try (ServerSocket serverSocket = new ServerSocket()) {
            if (SO_RCVBUF.isPresent())
                serverSocket.setOption(StandardSocketOptions.SO_RCVBUF, SO_RCVBUF.get());
            serverSocket.bind(new InetSocketAddress(PORT));
            for (; ; ) {
                try (Socket socket = serverSocket.accept()) {
                    InputStream in = NO_BUFSTREAM ? socket.getInputStream()
                            : new BufferedInputStream(socket.getInputStream());
                    int count = 0;
                    while (in.read(b4) == 4) {
                        int length = (b4[0] & 255) | (b4[1] & 255) << 8 | (b4[2] & 255) << 16 | (b4[3] & 255) << 32;
                        count++;
                        if (PATH.isPresent()) {
                            try (OutputStream out = Files.newOutputStream(PATH.get())) {
                                while (length > 0) {
                                    byte[] b = supplier.get();
                                    int read;
                                    int pos = 0;
                                    int remaining = Math.min(b.length, length);
                                    while (remaining > 0 && (read = in.read(b, pos, remaining)) > 0) {
                                        remaining -= read;
                                        pos += read;
                                    }
                                    out.write(b, 0, pos);
                                    length -= pos;
                                }
                            }
                        } else {
                            in.skipNBytes(length);
                        }
                    }
                    System.out.printf("Received %d objects%n", count);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
