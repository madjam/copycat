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
package net.kuujo.copycat.cluster.internal.coordinator;

import com.typesafe.config.ConfigValueFactory;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.util.AbstractConfigurable;
import net.kuujo.copycat.util.Configurable;
import net.kuujo.copycat.util.concurrent.NamedThreadFactory;
import net.kuujo.copycat.util.internal.Assert;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Copycat configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CoordinatorConfig extends AbstractConfigurable {
  public static final String COORDINATOR_NAME = "name";
  public static final String COORDINATOR_CLUSTER = "cluster";
  public static final String COORDINATOR_EXECUTOR = "executor";

  private final Executor DEFAULT_COORDINATOR_EXECUTOR = Executors.newSingleThreadExecutor(new NamedThreadFactory("copycat-coordinator-%d"));

  private ClusterConfig clusterConfig;
  private Executor executor;

  public CoordinatorConfig() {
    super();
  }

  public CoordinatorConfig(CoordinatorConfig config) {
    super(config);
  }

  public CoordinatorConfig(Map<String, Object> config) {
    super(config);
  }

  @Override
  public CoordinatorConfig copy() {
    return new CoordinatorConfig(this);
  }

  /**
   * Sets the Copycat instance name.
   *
   * @param name The Copycat instance name.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public void setName(String name) {
    this.config = config.withValue(COORDINATOR_NAME, ConfigValueFactory.fromAnyRef(Assert.isNotNull(name, "name")));
  }

  /**
   * Returns the Copycat instance name.
   *
   * @return The Copycat instance name.
   */
  public String getName() {
    return config.getString(COORDINATOR_NAME);
  }

  /**
   * Sets the Copycat instance name, returning the configuration for method chaining.
   *
   * @param name The Copycat instance name.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the name is {@code null}
   */
  public CoordinatorConfig withName(String name) {
    setName(name);
    return this;
  }

  /**
   * Sets the Copycat cluster configuration.
   *
   * @param cluster The Copycat cluster configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public void setClusterConfig(ClusterConfig cluster) {
    this.config = config.withValue(COORDINATOR_CLUSTER, ConfigValueFactory.fromMap(Assert.isNotNull(cluster, "config").toMap()));
    this.clusterConfig = cluster;
  }

  /**
   * Returns the Copycat cluster configuration.
   *
   * @return The Copycat cluster configuration.
   */
  public ClusterConfig getClusterConfig() {
    return clusterConfig != null ? clusterConfig : Configurable.load(config.getObject(COORDINATOR_CLUSTER).unwrapped());
  }

  /**
   * Sets the Copycat cluster configuration, returning the Copycat configuration for method chaining.
   *
   * @param config The Copycat cluster configuration.
   * @return The Copycat configuration.
   * @throws java.lang.NullPointerException If the cluster configuration is {@code null}
   */
  public CoordinatorConfig withClusterConfig(ClusterConfig config) {
    setClusterConfig(config);
    return this;
  }

  /**
   * Sets the coordinator executor.
   *
   * @param executor The coordinator executor.
   */
  public void setExecutor(Executor executor) {
    this.executor = executor;
  }

  /**
   * Returns the coordinator executor.
   *
   * @return The coordinator executor or {@code null} if no executor was specified.
   */
  public Executor getExecutor() {
    return executor != null ? executor : DEFAULT_COORDINATOR_EXECUTOR;
  }

  /**
   * Sets the coordinator executor, returning the configuration for method chaining.
   *
   * @param executor The coordinator executor.
   * @return The coordinator configuration.
   */
  public CoordinatorConfig withExecutor(Executor executor) {
    setExecutor(executor);
    return this;
  }

}
