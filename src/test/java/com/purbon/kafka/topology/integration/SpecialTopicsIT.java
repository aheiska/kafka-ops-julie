package com.purbon.kafka.topology.integration;

import static com.purbon.kafka.topology.CommandLineInterface.BROKERS_OPTION;
import static com.purbon.kafka.topology.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.purbon.kafka.topology.*;
import com.purbon.kafka.topology.api.adminclient.TopologyBuilderAdminClient;
import com.purbon.kafka.topology.exceptions.RemoteValidationException;
import com.purbon.kafka.topology.integration.containerutils.ContainerFactory;
import com.purbon.kafka.topology.integration.containerutils.ContainerTestUtils;
import com.purbon.kafka.topology.integration.containerutils.SaslPlaintextKafkaContainer;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.schemas.SchemaRegistryManager;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.junit.*;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class SpecialTopicsIT {

  private static SaslPlaintextKafkaContainer container;
  private static AdminClient kafkaAdminClient;
  private TopicManager topicManager;

  private ExecutionPlan plan;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @BeforeClass
  public static void setup() {
    container = ContainerFactory.fetchSaslKafkaContainer(System.getProperty("cp.version"));
    container.start();
    ContainerTestUtils.resetAcls(container);
    kafkaAdminClient = ContainerTestUtils.getSaslJulieAdminClient(container);
  }

  @AfterClass
  public static void teardown() {
    container.stop();
    kafkaAdminClient.close(Duration.ZERO);
  }

  @Before
  public void before() throws IOException {
    Files.deleteIfExists(Paths.get(".cluster-state"));

    TopologyBuilderAdminClient adminClient = new TopologyBuilderAdminClient(kafkaAdminClient);

    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final SchemaRegistryManager schemaRegistryManager =
        new SchemaRegistryManager(schemaRegistryClient, System.getProperty("user.dir"));
    this.plan = ExecutionPlan.init(new BackendController(), System.out);

    Properties props = new Properties();
    props.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    props.put(ALLOW_DELETE_TOPICS, true);

    var cliOps = Map.of(BROKERS_OPTION, "");

    Configuration config = new Configuration(cliOps, props);

    this.topicManager = new TopicManager(adminClient, schemaRegistryManager, config);
  }

  @Test
  public void testSpecialTopicCreation()
      throws ExecutionException, InterruptedException, IOException {

    Topology topology = new TopologyImpl();
    Map<String, String> config = buildDummyTopicConfig();
    Topic topicA = new Topic("configured-topic-a", config);
    Topic topicB = new Topic("configured-topic-b", config);

    topology.addSpecialTopic(topicA);
    topology.addSpecialTopic(topicB);

    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopics(Arrays.asList(topicA.toString(), topicB.toString()));
  }

  @Test(expected = RemoteValidationException.class)
  public void topicManagerShouldDetectDeletedSpecialTopicsBetweenRuns() throws IOException {

    TopologyBuilderAdminClient adminClient = new TopologyBuilderAdminClient(kafkaAdminClient);

    final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    final SchemaRegistryManager schemaRegistryManager =
        new SchemaRegistryManager(schemaRegistryClient, System.getProperty("user.dir"));

    Properties props = new Properties();
    props.put(TOPOLOGY_TOPIC_STATE_FROM_CLUSTER, "false");
    props.put(ALLOW_DELETE_TOPICS, true);
    props.put(JULIE_VERIFY_STATE_SYNC, true);

    HashMap<String, String> cliOps = new HashMap<>();
    cliOps.put(BROKERS_OPTION, "");

    Configuration config = new Configuration(cliOps, props);

    this.topicManager = new TopicManager(adminClient, schemaRegistryManager, config);

    Topic topic1 = new Topic("special-topic1-to-be-deleted");
    Topic topic2 = new Topic("special-topic2-not-deleted");

    Topology topology = new TopologyImpl();
    topology.addSpecialTopic(topic1);
    topology.addSpecialTopic(topic2);

    topicManager.updatePlan(topology, plan);
    plan.run();

    adminClient.deleteTopics(List.of(topic1.getName()));
    topicManager.updatePlan(topology, plan);
  }

  @Test
  public void testSpecialTopicCreationWithDefaultTopicConfigs()
      throws IOException, ExecutionException, InterruptedException {

    Topology topology = new TopologyImpl();

    Topic topicA = new Topic("special-topic-a-with-default-config");
    Topic topicB = new Topic("special-topic-b-with-default-config");

    topology.addSpecialTopic(topicA);
    topology.addSpecialTopic(topicB);

    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopics(List.of(topicA.toString(), topicB.toString()));
  }

  @Test(expected = IOException.class)
  public void testSpecialTopicCreationWithBadConfig() throws IOException {
    Map<String, String> config =
        Map.of(
            TopicManager.NUM_PARTITIONS,
            "1",
            TopicManager.REPLICATION_FACTOR,
            "1",
            "banana",
            "bar");

    Topology topology = new TopologyImpl();

    Topic topicA = new Topic("test-special-topic-creation-with-bad-config", config);
    topicA.setConfig(config);

    topology.addSpecialTopic(topicA);

    topicManager.updatePlan(topology, plan);
    plan.run();
  }

  @Test
  public void testSpecialTopicDelete()
      throws ExecutionException, InterruptedException, IOException {

    Topic topicA = new Topic("special-topic-a-in-topic-delete-stays", buildDummyTopicConfig());
    Topic topicB =
        new Topic("special-topic-b-in-topic-delete-gets-removed", buildDummyTopicConfig());

    Topology topology = new TopologyImpl();
    topology.addSpecialTopic(topicA);
    topology.addSpecialTopic(topicB);

    var internalTopic = createInternalTopic();

    topicManager.updatePlan(topology, plan);
    plan.run();

    Topic topicC = new Topic("special-topic-c-in-topic-delete-is-new", buildDummyTopicConfig());

    topology = new TopologyImpl();
    topology.addSpecialTopic(topicA);
    topology.addSpecialTopic(topicC);

    plan.getActions().clear();
    topicManager.updatePlan(topology, plan);
    plan.run();

    Set<String> topicNames = kafkaAdminClient.listTopics().names().get();

    assertThat(topicNames).contains(topicA.toString(), topicC.toString(), internalTopic);
    assertThat(topicNames).doesNotContain(topicB.toString());
  }

  @Test
  public void testSpecialTopicCreationWithConfig()
      throws ExecutionException, InterruptedException, IOException {

    Topology topology = new TopologyImpl();

    var config = new HashMap<>(buildDummyTopicConfig());
    config.put("retention.bytes", "104857600"); // set the retention.bytes per partition to 100mb
    Topic topicA = new Topic("topicA-created-with-config", config);
    topology.addSpecialTopic(topicA);

    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopicConfiguration(topicA.toString(), config);
  }

  @Test
  public void testSpecialTopicConfigUpdate()
      throws ExecutionException, InterruptedException, IOException {

    var config = new HashMap<>(buildDummyTopicConfig());
    config.put("retention.bytes", "104857600"); // set the retention.bytes per partition to 100mb
    config.put("segment.bytes", "104857600");
    Topology topology = new TopologyImpl();
    Topic topicA = new Topic("special-topicA-config-updated", config);
    topology.addSpecialTopic(topicA);

    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopicConfiguration(topicA.toString(), config);

    topology = new TopologyImpl();
    config = new HashMap<>(buildDummyTopicConfig());
    config.put("retention.bytes", "104");
    topicA = new Topic("special-topicA-config-updated", config);
    topology.addSpecialTopic(topicA);

    plan.getActions().clear();
    topicManager.updatePlan(topology, plan);
    plan.run();

    verifyTopicConfiguration(topicA.toString(), config, Collections.singletonList("segment.bytes"));
  }

  @Test
  public void testTopicCreationWithSpecialTopics()
      throws ExecutionException, InterruptedException, IOException {

    Topology topology = new TopologyImpl();
    topology.setContext("testSpecialTopicCreationWithRoles");

    Project project = new ProjectImpl("project");
    topology.addProject(project);

    var topicConfig = buildDummyTopicConfig();

    Topic topicA = new Topic("topicA", topicConfig);
    project.addTopic(topicA);

    var special = new Topic("i-am-special", topicConfig);
    var specialTopics = List.of(special);
    topology.setSpecialTopics(specialTopics);

    topicManager.updatePlan(topology, plan);
    plan.run();

    Set<String> topicNames = kafkaAdminClient.listTopics().names().get();

    assertThat(topicNames).contains(topicA.toString());
    assertThat(topicNames).contains(special.toString());
    assertEquals("Should create 2 new topics", 2, topicNames.size());
  }

  private void verifyTopicConfiguration(String topic, HashMap<String, String> config)
      throws ExecutionException, InterruptedException {
    verifyTopicConfiguration(topic, config, new ArrayList<>());
  }

  private void verifyTopicConfiguration(
      String topic, HashMap<String, String> config, List<String> removedConfigs)
      throws ExecutionException, InterruptedException {

    ConfigResource resource = new ConfigResource(Type.TOPIC, topic);
    Collection<ConfigResource> resources = Collections.singletonList(resource);

    Map<ConfigResource, Config> configs = kafkaAdminClient.describeConfigs(resources).all().get();

    Config topicConfig = configs.get(resource);
    Assert.assertNotNull(topicConfig);

    topicConfig
        .entries()
        .forEach(
            entry -> {
              if (!entry.isDefault()) {
                if (config.get(entry.name()) != null)
                  assertEquals(config.get(entry.name()), entry.value());
                Assert.assertFalse(removedConfigs.contains(entry.name()));
              }
            });
  }

  private Map<String, String> buildDummyTopicConfig() {
    return Map.of(TopicManager.NUM_PARTITIONS, "1", TopicManager.REPLICATION_FACTOR, "1");
  }

  private String createInternalTopic() {

    String topic = "_internal-topic";
    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

    try {
      kafkaAdminClient.createTopics(Collections.singleton(newTopic)).all().get();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return topic;
  }

  private void verifyTopics(List<String> topics) throws ExecutionException, InterruptedException {
    Set<String> topicNames = kafkaAdminClient.listTopics().names().get();
    topics.forEach(
        topic -> assertTrue("Topic " + topic + " not found", topicNames.contains(topic)));
  }
}
