package com.purbon.kafka.topology.integration.containerutils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public final class ContainerFactory {

  private static final Logger LOGGER = LogManager.getLogger(ContainerFactory.class);

  public static SaslPlaintextKafkaContainer fetchSaslKafkaContainer(String version) {
    LOGGER.debug("Fetching SASL Kafka Container with version=" + version);
    SaslPlaintextKafkaContainer container = null;
    if (version == null || version.isEmpty()) {
      container = new SaslPlaintextKafkaContainer();
    } else {
      DockerImageName containerImage =
          DockerImageName.parse("confluentinc/cp-kafka").withTag(version);
      container = new SaslPlaintextKafkaContainer(containerImage);
    }

    container.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*Kafka Server started.*"));

    return container;
  }
}
