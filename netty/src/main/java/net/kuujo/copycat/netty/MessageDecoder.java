package net.kuujo.copycat.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decoder for inbound messages.
 */
public class MessageDecoder extends ReplayingDecoder<DecoderState> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private int contentLength;
    
    public MessageDecoder() {
    	super(DecoderState.READ_CONTENT_LENGTH);
    }

    @Override
    protected void decode(
            ChannelHandlerContext context,
            ByteBuf buffer,
            List<Object> out) throws Exception {

        switch (state()) {
        case READ_CONTENT_LENGTH:
            contentLength = buffer.readInt();
            checkpoint(DecoderState.READ_CONTENT);
        case READ_CONTENT:
        	out.add(buffer.readBytes(contentLength));
            checkpoint(DecoderState.READ_CONTENT_LENGTH);
            break;
         default:
            throw new IllegalStateException("Must not be here");
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        log.error("Exception inside channel handling pipeline.", cause);
        context.close();
    }
}