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
package net.kuujo.copycat.log;

import java.io.IOException;
import java.util.TreeMap;

import net.kuujo.copycat.util.internal.Assert;

/**
 * Log manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface LogManager extends Loggable {

  /**
   * Returns the log configuration.
   *
   * @return The log configuration.
   */
  LogConfig config();

  /**
   * Returns a map of all segments in the log.
   *
   * @return A map of segments in the log.
   */
  TreeMap<Long, LogSegment> segments();

  /**
   * Returns the current log segment.
   */
  LogSegment segment();

  /**
   * Returns a log segment by index.
   *
   * @throws IndexOutOfBoundsException if no segment exists for the {@code index}
   */
  LogSegment segment(long index);

  /**
   * Returns the first log segment.
   */
  LogSegment firstSegment();

  /**
   * Returns the last log segment.
   */
  LogSegment lastSegment();

  /**
   * Forces the log to roll over to a new segment.
   *
   * @throws IOException If the log failed to create a new segment.
   */
  void rollOver(long index) throws IOException;

  /**
   * Compacts the log, removing all segments up to and including the given index.
   *
   * @param index The index to which to compact the log. This must be the first index of the last
   *          segment in the log to remove via compaction
   * @throws IllegalArgumentException if {@code index} is not the first index of a segment or if
   *           {@code index} represents the last segment in the log
   * @throws IndexOutOfBoundsException if {@code index} is out of bounds for the log
   * @throws IOException If the log failed to delete a segment.
   */
  void compact(long index) throws IOException;

  /**
   * Splits log at the specified index.
   * All log entries after the specified index will be moved to a new segment.
   *
   * @throws IOException If the log failed to split
   */
  void split(long index) throws IOException;
}
