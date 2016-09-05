/*
 * Copyright (c) 2016, 8Kdata Technology S.L.
 *
 * Permission to use, copy, modify, and distribute this software and its documentation for any purpose,
 * without fee, and without a written agreement is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL 8Kdata BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES,
 * INCLUDING LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF 8Kdata HAS BEEN
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * 8Kdata SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS,
 * AND 8Kdata HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */


package com.eightkdata.twentypercent.tiot1.adsbbsb1.receiver;


import com.eightkdata.twentypercent.tiot1.adsbbsb1.receiver.util.SimpleNamedThreadFactory;
import com.lmax.disruptor.dsl.Disruptor;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.PublishProcessor;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class BSB1Client {
    private static final int MAX_CSV_LINE_LENGTH = 1024;    //Way more than in reality
    private static final int DEFAULT_PORT = 30003;
    private static final int NETTY_THREADS_DEFAULT_NUMBER = 0;  // See MultithreadEventLoopGroup.java
    private static final String NETTY_THREADS_NAME_PREFIX = "adsbbsb1-receiver-netty";
    private static final String DISRUPTOR_THREADS_NAME_PREFIX = "adsbbsb1-receiver-disruptor";
    private static final short DISRPUTOR_BUFFER_SIZE_POWER_2_EXPONENT = 10;     // = 1024
    private static final int MAX_SECONDS_BETWEEN_SERVER_RECONNECTS = 10;

    private final Logger logger = LoggerFactory.getLogger(BSB1Client.class);

    private final InetAddress host;
    private final int port;
    private final FlowableProcessor<BSB1CSVMessage> processor = PublishProcessor.create();
    private final Disruptor<BSB1CSVMessage> disruptor;
    private boolean connectionSuccessful;

    private BSB1Client(@Nonnull InetAddress host, @Nonnegative int port, @Nonnegative int bufferSize) {
        this.host = host;
        this.port = port;

        disruptor = new Disruptor<>(
                BSB1CSVMessage::new,
                bufferSize,
                new SimpleNamedThreadFactory(DISRUPTOR_THREADS_NAME_PREFIX)
        );
        disruptor.handleEventsWith(
                (message, sequence, endOfBatch) ->  processor.onNext(message)
        );
        disruptor.start();

        logger.info("ADS-B BSB-1 receiver client initialized. Server: {}:{}", host, port);
    }

    public static class Builder {
        private InetAddress host;
        private int port;
        private int bufferSize;

        private Builder() {}

        public Builder host(@Nonnull String host) {
            checkNotNull(host);
            try {
                this.host = InetAddress.getByName(host);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid host '" + host + "'");
            }

            return this;
        }

        public Builder port(@Nonnegative int port) {
            checkArgument(port > 0 && port <= 65535, "Invalid port. Must be in the (0,65535] range");
            this.port = port;

            return this;
        }

        public Builder bufferSize(@Nonnegative int bufferSize) {
            checkArgument(
                    bufferSize > 0 && (bufferSize & (bufferSize - 1)) == 0,
                    "Buffer size must be positive and a power of two"
            );
            this.bufferSize = bufferSize;

            return this;
        }

        public BSB1Client get() {
            return new BSB1Client(
                    null != host ? host : InetAddress.getLoopbackAddress(),
                    port != 0 ? port : DEFAULT_PORT,
                    bufferSize != 0 ? bufferSize : 1 << DISRPUTOR_BUFFER_SIZE_POWER_2_EXPONENT
            );
        }
    }

    public static Builder instance() {
        return new Builder();
    }

    public Publisher<BSB1CSVMessage> publisher() {
        return processor;
    }

    public void start() {
        while(true) {
            connectionSuccessful = false;
            try {
                connect();
                if(!connectionSuccessful) {
                    Thread.sleep((long) (Math.random() * MAX_SECONDS_BETWEEN_SERVER_RECONNECTS * 1000));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();     // Clear interrupt flag
                processor.onComplete();
                logger.info("Client start() method was interrupted. Exiting method");
                break;
            }
        }
    }

    private void connect() throws InterruptedException {
        EventLoopGroup workerGroup = new NioEventLoopGroup(
                NETTY_THREADS_DEFAULT_NUMBER,
                new SimpleNamedThreadFactory(NETTY_THREADS_NAME_PREFIX)
        );

        Bootstrap bootstrap = new Bootstrap()
                .group(workerGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast("Frame Decoder", new LineBasedFrameDecoder(MAX_CSV_LINE_LENGTH));
                        ch.pipeline().addLast("String Decoder", new StringDecoder(CharsetUtil.US_ASCII));
                        ch.pipeline().addLast("BSB1 Decoder", new BSB1ClientHandler(disruptor.getRingBuffer()));
                    }
                })
        ;

        // Start the client.
        ChannelFuture channelFuture = bootstrap.connect(host, port);
        channelFuture.addListener(
                (ChannelFutureListener) c -> {
                    if(c.isSuccess()) {
                        connectionSuccessful = true;
                        logger.info(
                                "Connected to server {}:{} from port {}", host, port,
                                ((InetSocketAddress) c.channel().localAddress()).getPort()
                        );
                    } else {
                        connectionSuccessful = false;
                        logger.info("Could not connect to server {}:{}", host, port);
                    }
                }
        );

        try {
            channelFuture.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();

            if(connectionSuccessful) {
                logger.info("Server {}:{} disconnected", host, port);
            }
        }
    }
}
