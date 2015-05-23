/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.cluster;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;

import java.net.InetSocketAddress;

/**
 * Netty member info.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyMemberInfo extends MemberInfo {
  InetSocketAddress address;

  public NettyMemberInfo() {
  }

  public NettyMemberInfo(int id, InetSocketAddress address) {
    super(id);
    this.address = address;
  }

  /**
   * Returns the member address.
   *
   * @return The member address.
   */
  public InetSocketAddress address() {
    return address;
  }

  @Override
  public void writeObject(Buffer buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeInt(address.getHostString().getBytes().length)
      .write(address.getHostString().getBytes())
      .writeInt(address.getPort());
  }

  @Override
  public void readObject(Buffer buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    byte[] bytes = new byte[buffer.readInt()];
    buffer.read(bytes);
    address = new InetSocketAddress(new String(bytes), buffer.readInt());
  }

}
