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


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.util.CharsetUtil;
import io.reactivex.processors.FlowableProcessor;
import io.reactivex.processors.PublishProcessor;
import org.reactivestreams.Publisher;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class BSB1Client {
    private static final int MAX_CSV_LINE_LENGTH = 1024;    //Way more than in reality
    private static final int DEFAULT_PORT = 30003;

    private final InetAddress host;
    private final int port;
    private final FlowableProcessor<BSB1CSVMessage> processor = PublishProcessor.create();

    private BSB1Client(@Nonnull InetAddress host, @Nonnegative int port) {
        this.host = host;
        this.port = port;
    }

    public static class Builder {
        private InetAddress host;
        private int port;

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

        public BSB1Client get() {
            return new BSB1Client(
                    null != host ? host : InetAddress.getLoopbackAddress(),
                    port != 0 ? port : DEFAULT_PORT
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
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                    ch.pipeline().addLast("Frame Decoder", new LineBasedFrameDecoder(MAX_CSV_LINE_LENGTH));
                    ch.pipeline().addLast("String Decoder", new StringDecoder(CharsetUtil.US_ASCII));
                    ch.pipeline().addLast("BSB1 Decoder", new BSB1ClientHandler(processor));
                }
            });

            // Start the client.
            ChannelFuture f = b.connect(host, port).sync();

            // Wait until the connection is closed.
            f.channel().closeFuture().sync();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            workerGroup.shutdownGracefully();
            processor.onComplete();
        }
    }
}
