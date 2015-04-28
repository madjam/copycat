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
package net.kuujo.copycat.util.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.typesafe.config.ConfigValueFactory;

import net.kuujo.copycat.cluster.internal.MemberInfo;
import net.kuujo.copycat.raft.protocol.AppendRequest;
import net.kuujo.copycat.raft.protocol.AppendResponse;
import net.kuujo.copycat.raft.protocol.CommitRequest;
import net.kuujo.copycat.raft.protocol.CommitResponse;
import net.kuujo.copycat.raft.protocol.PollRequest;
import net.kuujo.copycat.raft.protocol.PollResponse;
import net.kuujo.copycat.raft.protocol.QueryRequest;
import net.kuujo.copycat.raft.protocol.QueryResponse;
import net.kuujo.copycat.raft.protocol.ReplicaInfo;
import net.kuujo.copycat.raft.protocol.SyncRequest;
import net.kuujo.copycat.raft.protocol.SyncResponse;
import net.kuujo.copycat.raft.protocol.VoteRequest;
import net.kuujo.copycat.raft.protocol.VoteResponse;
import net.kuujo.copycat.util.ConfigurationException;
import net.kuujo.copycat.util.internal.Assert;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kryo serializer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KryoSerializer extends SerializerConfig {
  private static final String KRYO_SERIALIZER_BUFFER_SIZE = "buffer.size";
  private static final String KRYO_SERIALIZER_REGISTRATIONS = "registrations";

  private static final int DEFAULT_KRYO_SERIALIZER_BUFFER_SIZE = 1024 * 1024 * 16;

  private static Map<String, Object> idToClassMap = new HashMap<>();
  static {
    idToClassMap.put(String.valueOf(101), SyncRequest.class);
    idToClassMap.put(String.valueOf(102), SyncResponse.class);
    idToClassMap.put(String.valueOf(103), PollRequest.class);
    idToClassMap.put(String.valueOf(104), PollResponse.class);
    idToClassMap.put(String.valueOf(105), VoteRequest.class);
    idToClassMap.put(String.valueOf(106), VoteResponse.class);
    idToClassMap.put(String.valueOf(107), AppendRequest.class);
    idToClassMap.put(String.valueOf(108), AppendResponse.class);
    idToClassMap.put(String.valueOf(109), QueryRequest.class);
    idToClassMap.put(String.valueOf(110), QueryResponse.class);
    idToClassMap.put(String.valueOf(111), CommitRequest.class);
    idToClassMap.put(String.valueOf(112), CommitResponse.class);
    idToClassMap.put(String.valueOf(113), ReplicaInfo.class);
    idToClassMap.put(String.valueOf(114), MemberInfo.class);
  }

  private Kryo kryo = new Kryo();
  private ByteBufferOutput output;
  private ByteBufferInput input;
  private AtomicBoolean init = new AtomicBoolean();

  public KryoSerializer() {
    super();
  }

  public KryoSerializer(Map<String, Object> config) {
    super(config);
  }

  public KryoSerializer(String resource) {
    super(resource);
  }

  public KryoSerializer(KryoSerializer serializer) {
    super(serializer);
  }

  /**
   * Lazily initializes the serializer.
   */
  private void init() {
    if (init.compareAndSet(false, true)) {
      input = new ByteBufferInput();
      output = new ByteBufferOutput(ByteBuffer.allocateDirect(getBufferSize()));
      register();
    }
  }

  /**
   * Registers classes.
   */
  @SuppressWarnings({"rawtypes"})
  private void register() {
    Map<String, Object> regMap = config.hasPath(KRYO_SERIALIZER_REGISTRATIONS)
            ? this.config.getObject(KRYO_SERIALIZER_REGISTRATIONS).unwrapped()
            : idToClassMap;
    for (Map.Entry<String, Object> entry : regMap.entrySet()) {
      Object type = entry.getValue();
      if (type instanceof Class) {
        register((Class) type, Integer.valueOf(entry.getKey()));
      } else if (type instanceof String) {
        try {
          register(Class.forName(type.toString()), Integer.valueOf(entry.getKey()));
        } catch (ClassNotFoundException e) {
          throw new ConfigurationException("Failed to register serializer class", e);
        }
      }
    }
  }

  /**
   * Registers a class for serialization.
   *
   * @param type The class to register.
   * @return The Kryo serializer.
   */
  public synchronized KryoSerializer register(Class<?> type) {
    kryo.register(type);
    return this;
  }

  /**
   * Registers a class for serialization.
   *
   * @param type The class to register.
   * @param id The registration ID.
   * @return The Kryo serializer.
   */
  public synchronized KryoSerializer register(Class<?> type, int id) {
    kryo.register(type, id);
    return this;
  }

  /**
   * Sets the serializer buffer size.
   *
   * @param bufferSize The serializer buffer size.
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
   */
  public void setBufferSize(int bufferSize) {
    this.config = config.withValue(KRYO_SERIALIZER_BUFFER_SIZE, ConfigValueFactory.fromAnyRef(Assert.arg(bufferSize, bufferSize > 0, "buffer size must be positive")));
  }

  /**
   * Returns the serializer buffer size.
   *
   * @return The serializer buffer size.
   */
  public int getBufferSize() {
    return config.hasPath(KRYO_SERIALIZER_BUFFER_SIZE) ? config.getInt(KRYO_SERIALIZER_BUFFER_SIZE) : DEFAULT_KRYO_SERIALIZER_BUFFER_SIZE;
  }

  /**
   * Sets the serializer buffer size.
   *
   * @param bufferSize The serializer buffer size.
   * @return The Kryo serializer.
   * @throws java.lang.IllegalArgumentException If the buffer size is not positive
   */
  public KryoSerializer withBufferSize(int bufferSize) {
    setBufferSize(bufferSize);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public synchronized <T> T readObject(ByteBuffer buffer) {
    init();
    input.setBuffer(buffer);
    return (T) kryo.readClassAndObject(input);
  }

  @Override
  public synchronized ByteBuffer writeObject(Object object) {
    init();
    kryo.writeClassAndObject(output, object);
    byte[] bytes = output.toBytes();
    output.clear();
    return ByteBuffer.wrap(bytes);
  }

}
