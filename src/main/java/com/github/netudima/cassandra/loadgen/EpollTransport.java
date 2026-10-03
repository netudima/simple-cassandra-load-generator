package com.github.netudima.cassandra.loadgen;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.DefaultNettyOptions;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.util.concurrent.Future;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Makes the driver use netty's native epoll transport instead of NIO.
 */
public final class EpollTransport {

    private EpollTransport() {
    }

    /** True when the native library loaded and epoll can actually be used here. */
    public static boolean isAvailable() {
        return Epoll.isAvailable();
    }

    /** Why epoll is unavailable, for the error message. */
    public static String unavailabilityCause() {
        Throwable t = Epoll.unavailabilityCause();
        return t == null ? "unknown" : t.toString();
    }

    /** A session builder whose context hands out epoll-backed Netty options. */
    public static CqlSessionBuilder sessionBuilder() {
        return new CqlSessionBuilder() {
            @Override
            protected DriverContext buildContext(
                    DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
                return new EpollDriverContext(configLoader, programmaticArguments);
            }
        };
    }

    private static final class EpollDriverContext extends DefaultDriverContext {
        EpollDriverContext(DriverConfigLoader configLoader, ProgrammaticArguments args) {
            super(configLoader, args);
        }

        @Override
        protected NettyOptions buildNettyOptions() {
            return new EpollNettyOptions(this);
        }
    }

    /**
     * Everything {@code DefaultNettyOptions} does, with the IO group swapped for
     * epoll.
     */
    private static final class EpollNettyOptions extends DefaultNettyOptions {

        private final EventLoopGroup epollGroup;
        private final int shutdownQuietPeriod;
        private final int shutdownTimeout;
        private final java.util.concurrent.TimeUnit shutdownUnit;

        EpollNettyOptions(InternalDriverContext context) {
            super(context);

            // Discard the NIO group the superclass just created
            super.ioEventLoopGroup().shutdownGracefully(0, 0, java.util.concurrent.TimeUnit.SECONDS);

            var config = context.getConfig().getDefaultProfile();
            int size = config.getInt(DefaultDriverOption.NETTY_IO_SIZE);
            boolean daemon = config.getBoolean(DefaultDriverOption.NETTY_DAEMON);
            this.shutdownQuietPeriod = config.getInt(DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD);
            this.shutdownTimeout = config.getInt(DefaultDriverOption.NETTY_IO_SHUTDOWN_TIMEOUT);
            this.shutdownUnit = java.util.concurrent.TimeUnit
                    .valueOf(config.getString(DefaultDriverOption.NETTY_IO_SHUTDOWN_UNIT));

            // SafeThreadFactory lets the driver detect blocking calls made from an
            // IO thread; keep it on our threads too.
            ThreadFactory safe = new BlockingOperation.SafeThreadFactory();
            AtomicInteger n = new AtomicInteger();
            String prefix = context.getSessionName() + "-epoll-io-";
            ThreadFactory factory = runnable -> {
                Thread t = safe.newThread(runnable);
                t.setName(prefix + n.getAndIncrement());
                t.setDaemon(daemon);
                return t;
            };
            this.epollGroup = new EpollEventLoopGroup(size, factory);
        }

        @Override
        public EventLoopGroup ioEventLoopGroup() {
            return epollGroup;
        }

        @Override
        public Class<? extends Channel> channelClass() {
            return EpollSocketChannel.class;
        }

        @Override
        public Future<Void> onClose() {
            // Ours first, then the superclass's admin group and timer.
            epollGroup.shutdownGracefully(shutdownQuietPeriod, shutdownTimeout, shutdownUnit);
            return super.onClose();
        }
    }

    /** For the settings banner: which transport a session built here will use. */
    public static String describe(boolean useEpoll) {
        if (!useEpoll) {
            return "NIO (netty NioEventLoopGroup - the driver's default)";
        }
        return "native epoll (EpollEventLoopGroup + EpollSocketChannel)";
    }

    /** Sanity check that a live session really ended up on epoll. */
    public static String actualTransport(CqlSession session) {
        DriverContext ctx = session.getContext();
        if (ctx instanceof InternalDriverContext internal) {
            return internal.getNettyOptions().channelClass().getSimpleName();
        }
        return "unknown";
    }
}
