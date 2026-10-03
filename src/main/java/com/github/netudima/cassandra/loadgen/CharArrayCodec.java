package com.github.netudima.cassandra.loadgen;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;

/**
 * Binds {@code char[]} straight to a CQL {@code text} column.
 *
 * <p>{@code setString} would allocate three objects per cell: the String, the
 * {@code byte[]} from {@code String.getBytes(UTF_8)}, and the {@code ByteBuffer}
 * wrapping it. Here the values are pre-built {@code char[]} from a pool, so
 * encoding is one ByteBuffer and a copy loop.
 *
 * <p><b>ASCII only.</b> {@link #encode} narrows each char to one byte, which is
 * correct for the generated alphabet ([A-Za-z0-9]) and wrong for anything above
 * U+007F.
 */
public final class CharArrayCodec implements TypeCodec<char[]> {

    public static final CharArrayCodec INSTANCE = new CharArrayCodec();

    private static final GenericType<char[]> JAVA_TYPE = GenericType.of(char[].class);

    private CharArrayCodec() {
    }

    @Override
    public GenericType<char[]> getJavaType() {
        return JAVA_TYPE;
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.TEXT;
    }

    @Override
    public ByteBuffer encode(char[] value, ProtocolVersion protocolVersion) {
        if (value == null) {
            return null;
        }
        ByteBuffer out = ByteBuffer.allocate(value.length);
        for (char c : value) {
            out.put((byte) c);
        }
        return out.flip();
    }

    @Override
    public char[] decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        if (bytes == null) {
            return null;
        }
        // duplicate(): decoding must not disturb the caller's position
        ByteBuffer in = bytes.duplicate();
        char[] out = new char[in.remaining()];
        for (int i = 0; i < out.length; i++) {
            out[i] = (char) (in.get() & 0xFF);
        }
        return out;
    }

    @Override
    public String format(char[] value) {
        return value == null ? "NULL" : "'" + new String(value).replace("'", "''") + "'";
    }

    @Override
    public char[] parse(String value) {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
            return null;
        }
        String s = value;
        if (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'') {
            s = s.substring(1, s.length() - 1).replace("''", "'");
        }
        return s.toCharArray();
    }
}
