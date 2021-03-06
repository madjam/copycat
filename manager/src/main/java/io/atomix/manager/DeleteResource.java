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
package io.atomix.manager;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.client.Command;

/**
 * Delete resource command.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=37)
public class DeleteResource implements Command<Boolean>, CatalystSerializable {
  private long resource;

  public DeleteResource() {
  }

  public DeleteResource(long resource) {
    this.resource = resource;
  }

  @Override
  public PersistenceLevel persistence() {
    return PersistenceLevel.PERSISTENT;
  }

  /**
   * Returns the resource ID.
   *
   * @return The resource ID.
   */
  public long resource() {
    return resource;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(resource);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    resource = buffer.readLong();
  }

}
