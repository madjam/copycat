/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;

import javax.net.ssl.SSLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Netty TCP protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyTcpProtocolClient implements ProtocolClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTcpProtocolClient.class);
  private final String host;
  private final int port;
  private final NettyTcpProtocol protocol;
  private final EventLoopGroup group;
  private Channel channel;
  ChannelHandlerContext context;
  private final Cache<Object, CompletableFuture<ByteBuffer>> responseFutures = CacheBuilder.newBuilder()
          .maximumSize(10000)
          .expireAfterWrite(2, TimeUnit.SECONDS)
          .removalListener(new RemovalListener<Object, CompletableFuture<ByteBuffer>>() {
              @Override
              public void onRemoval(RemovalNotification<Object, CompletableFuture<ByteBuffer>> entry) {
                entry.getValue().completeExceptionally(new TimeoutException());
              }
          })
          .build();
  private long requestId;
  private static final ScheduledExecutorService CONNECTION_AGENT =
		  Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("netty-tcp-connection-monitor-%d"));

  public NettyTcpProtocolClient(String host, int port, NettyTcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
    this.group = new NioEventLoopGroup(protocol.getThreads());
    CONNECTION_AGENT.scheduleWithFixedDelay((() -> connect()), 0, 100, TimeUnit.MILLISECONDS);
  }

  @Override
  public CompletableFuture<ByteBuffer> write(ByteBuffer request) {
    final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    if (channel != null) {
      request.rewind();
      long requestId = ++this.requestId;
      ByteBuf requestBuffer = context.alloc().buffer(request.remaining() + 12);
      requestBuffer.writeLong(requestId);
      requestBuffer.writeBytes(request);
      responseFutures.put(requestId, future);
      channel.writeAndFlush(requestBuffer).addListener((channelFuture) -> {
        if (!channelFuture.isSuccess()) {
          future.completeExceptionally(new ProtocolException(channelFuture.cause()));
        }
      });
    } else {
      future.completeExceptionally(new ProtocolException("Client not connected"));
    }
    return future;
  }

  @Override
  public CompletableFuture<Void> connect() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (channel != null && channel.isActive()) {
      future.complete(null);
      return future;
    }

    final SslContext sslContext;
    if (protocol.isSsl()) {
      try {
        sslContext = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
      } catch (SSLException e) {
        future.completeExceptionally(e);
        return future;
      }
    } else {
      sslContext = null;
    }

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(group)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          if (sslContext != null) {
            pipeline.addLast(sslContext.newHandler(channel.alloc(), host, port));
          }
          pipeline.addLast(
            //new ObjectEncoder(),
            //new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(getClass().getClassLoader())),
            new MessageEncoder(),
            new MessageDecoder(),
            new TcpProtocolClientHandler(NettyTcpProtocolClient.this)
          );
        }
      });

    if (protocol.getSendBufferSize() > -1) {
      bootstrap.option(ChannelOption.SO_SNDBUF, protocol.getSendBufferSize());
    }

    if (protocol.getReceiveBufferSize() > -1) {
      bootstrap.option(ChannelOption.SO_RCVBUF, protocol.getReceiveBufferSize());
    }

    if (protocol.getTrafficClass() > -1) {
      bootstrap.option(ChannelOption.IP_TOS, protocol.getTrafficClass());
    }

    bootstrap.option(ChannelOption.TCP_NODELAY, true);
    bootstrap.option(ChannelOption.SO_LINGER, protocol.getSoLinger());
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, protocol.getConnectTimeout());

    bootstrap.connect(host, port).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture channelFuture) throws Exception {
        if (channelFuture.isSuccess()) {
          channel = channelFuture.channel();
          future.complete(null);
        } else  {
          future.completeExceptionally(channelFuture.cause());
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    final CompletableFuture<Void> future = new CompletableFuture<>();
    if (channel != null) {
      channel.close().addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
          channel = null;
          if (channelFuture.isSuccess()) {
            future.complete(null);
          } else {
            future.completeExceptionally(channelFuture.cause());
          }
        }
      });
    } else {
      future.complete(null);
    }
    return future;
  }

  /**
   * Client response handler.
   */
  private static class TcpProtocolClientHandler extends ChannelInboundHandlerAdapter {
    private final NettyTcpProtocolClient client;

    private TcpProtocolClientHandler(NettyTcpProtocolClient client) {
      this.client = client;
    }

    public void channelActive(final ChannelHandlerContext context) {
      client.context = context;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void channelRead(final ChannelHandlerContext context, Object message) {
      ByteBuf response = (ByteBuf) message;
      long requestId = response.readLong();
      CompletableFuture responseFuture = client.responseFutures.getIfPresent(requestId);
      if (responseFuture != null) {
        responseFuture.complete(response.slice().nioBuffer());
        client.responseFutures.invalidate(requestId);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
      context.close();
    }
  }
}
