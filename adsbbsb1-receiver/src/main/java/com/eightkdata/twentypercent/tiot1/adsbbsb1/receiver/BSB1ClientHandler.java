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


import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;


public class BSB1ClientHandler extends MessageToMessageDecoder<String> {
    private final static EventTranslatorOneArg<BSB1CSVMessage, String> EVENT_TRANSLATOR =
            (message, sequence, str) -> message.setCSVMessage(str);

    private final Logger logger = LoggerFactory.getLogger(BSB1ClientHandler.class);

    private final RingBuffer<BSB1CSVMessage> ringBuffer;

    public BSB1ClientHandler(@Nonnull RingBuffer<BSB1CSVMessage> ringBuffer) {
        this.ringBuffer = checkNotNull(ringBuffer);
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, String s, List<Object> list) throws Exception {
        // Apply backpressure: if buffer is full, message is discarded
        if (ringBuffer.remainingCapacity() > 0) {
            ringBuffer.publishEvent(EVENT_TRANSLATOR, s);
        } else {
            logger.warn("Applying backpressure. Received message dropped, RingBuffer is full");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // TODO: log cause. Restart client?
        ctx.close();
        logger.error("Exception in channel handler", cause);
    }
}
