package com.github.netudima.cassandra.loadgen;

import com.datastax.oss.driver.api.core.ProtocolVersion;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Values are encoded once at startup through {@link CharArrayCodec}, then the
 * same {@link ByteBuffer} instance is bound on every request.
 *
 * <p>Sharing one buffer across concurrent requests is safe: the driver's protocol
 * codec reads {@code bytes.remaining()} and writes {@code bytes.duplicate()}, so
 * it never moves the position of the buffer it is given. No per-bind
 * {@code duplicate()} is needed.
 */
public final class ValuePool {

    /** ASCII only: {@link CharArrayCodec} narrows char to byte. */
    private static final char[] ALPHABET =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".toCharArray();

    /** Wire-format values, shared read-only across shards. */
    private final ByteBuffer[] encoded;

    public ValuePool(int size, int cellSize) {
        if (size < 1) {
            throw new IllegalArgumentException("value pool size must be >= 1");
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        this.encoded = new ByteBuffer[size];
        char[] buf = new char[cellSize];
        for (int i = 0; i < size; i++) {
            for (int j = 0; j < cellSize; j++) {
                buf[j] = ALPHABET[rnd.nextInt(ALPHABET.length)];
            }
            encoded[i] = CharArrayCodec.INSTANCE.encode(buf, ProtocolVersion.DEFAULT);
        }
    }

    /** A pre-encoded value, ready to bind with {@code setBytesUnsafe}. */
    public ByteBuffer value(ThreadLocalRandom rnd) {
        return encoded[rnd.nextInt(encoded.length)];
    }

    public int size() {
        return encoded.length;
    }

    /** Approximate retained size of the pool, for the settings banner. */
    public long retainedBytes() {
        return encoded.length * (long) (encoded[0].capacity() + 48);
    }
}
