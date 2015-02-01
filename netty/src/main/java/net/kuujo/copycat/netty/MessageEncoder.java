package net.kuujo.copycat.netty;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Encode InternalMessage out into a byte buffer.
 */
@Sharable
public class MessageEncoder extends MessageToByteEncoder<ByteBuf> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    protected void encode(
            ChannelHandlerContext context,
            ByteBuf in,
            ByteBuf out) throws Exception {

        out.writeInt(in.readableBytes());
        out.writeBytes(in);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        if (cause instanceof IOException) {
            log.debug("IOException inside channel handling pipeline.", cause);
        } else {
            log.error("non-IOException inside channel handling pipeline.", cause);
        }
        context.close();
    }
}