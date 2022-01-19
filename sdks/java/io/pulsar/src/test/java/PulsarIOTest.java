import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.pulsar.PulsarIO;
import org.apache.beam.sdk.io.pulsar.PulsarMessage;
import org.apache.beam.sdk.io.pulsar.PulsarMessageCoder;
import org.apache.beam.sdk.io.pulsar.ReadFromPulsarDoFn;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@RunWith(JUnit4.class)
public class PulsarIOTest {

    private final String TOPIC = "PULSAR_IO_TEST";
    private final String SERVICE_URL = "pulsar://localhost:6650";
    public final String SERVICE_HTTP_URL = "http://localhost:8080";
    protected static PulsarContainer pulsarContainer;
    protected static PulsarClient client;

    private long endExpectedTime = 0;

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarIOTest.class);

    @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

   // @Rule public final transient TestStream.Builder<PulsarMessage> testStream =
     //       TestStream.create(PulsarMessageCoder.of());

    public List<PulsarMessage> produceMockMessages() throws PulsarClientException {
        client = initClient();
        Producer<byte[]> producer = client.newProducer()
                                        .topic(TOPIC)
                                        .create();
        Consumer<byte[]> consumer = client.newConsumer()
                .topic(TOPIC)
                .subscriptionName("produceMockMessageFn")
                .subscribe();
        int numElements = 101;
    List<PulsarMessage> inputs = new ArrayList<>();
        for (int i = 0; i < numElements; i++) {
            String msg = ("PULSAR_TEST_READFROMSIMPLETOPIC_" + i);
            producer.send(msg.getBytes(StandardCharsets.UTF_8));
            CompletableFuture<Message<byte[]>> future = consumer.receiveAsync();
            Message<byte[]> message = null;
            try {
                message = future.get(5, TimeUnit.SECONDS);
                if(i>=100) {
                    endExpectedTime = message.getPublishTime();
                } else {
                    inputs.add(new PulsarMessage(message.getTopicName(), message.getPublishTime(), message));
                }
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
                //e.printStackTrace();
            } catch (ExecutionException e) {
                System.out.println(e.getMessage());
                //e.printStackTrace();
            } catch (TimeoutException e) {
                System.out.println(e.getMessage());
                //e.printStackTrace();
            }
        }
        consumer.close();
        producer.close();
        client.close();
        return inputs;
    }

    private static PulsarClient initClient() throws PulsarClientException {
        return PulsarClient.builder()
                .serviceUrl(pulsarContainer.getPulsarBrokerUrl())
                //.enableTransaction(true)
                .build();
    }


    private static void setupPulsarContainer() {
        pulsarContainer = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:latest"));
        pulsarContainer.withCommand("bin/pulsar", "standalone");
        pulsarContainer.start();
    }

    @BeforeClass
    public static void setup() throws PulsarClientException {
        setupPulsarContainer();
        client = initClient();
    }
    @AfterClass
    public static void afterClass() {
        if(pulsarContainer != null) {
            pulsarContainer.stop();
        }
    }

    @Test
    @SuppressWarnings({"rawtypes"})
    public void testPulsarFunctionality() throws Exception {
        try(
                Consumer consumer = client.newConsumer()
                        .topic(TOPIC)
                        .subscriptionName("PulsarIO_IT")
                        .subscribe();
                Producer<byte[]> producer = client.newProducer()
                        .topic(TOPIC)
                        .create();
        ) {
            String message_txt = "testing pulsar functionality";
            producer.send(message_txt.getBytes(StandardCharsets.UTF_8));
            CompletableFuture<Message> future = consumer.receiveAsync();
            Message message = future.get(5, TimeUnit.SECONDS);
            System.out.println(new String(message.getData(), StandardCharsets.UTF_8));
            assertEquals(message_txt, new String(message.getData(), StandardCharsets.UTF_8));
            client.close();
        }
    }

    @Test
    public void testReadFromSimpleTopic() {
        try {
            List<PulsarMessage> inputsMock = produceMockMessages();
            PulsarIO.Read reader = PulsarIO.read()
                    .withClientUrl(pulsarContainer.getPulsarBrokerUrl())
                    .withAdminUrl(pulsarContainer.getHttpServiceUrl())
                    .withTopic(TOPIC)
                    .withEndTimestamp(endExpectedTime)
                    .withPublishTime();
            testPipeline
                    .apply(reader)
                    .apply(ParDo.of(new PulsarRecordsMetric()));
            PipelineResult pipelineResult =  testPipeline.run();
            MetricQueryResults metrics =  pipelineResult.metrics().queryMetrics(MetricsFilter.builder().addNameFilter(MetricNameFilter.named(PulsarIOTest.class.getName(), "PulsarRecordsCounter")).build());
            long recordsCount = 0;
            for(MetricResult<Long> metric:metrics.getCounters()) {
                if(metric.getName().toString().equals("PulsarIOTest:PulsarRecordsCounter")) {
                    recordsCount = metric.getAttempted();
                    break;
                }
            }
            assertEquals(inputsMock.size(), (int) recordsCount);

        } catch (PulsarClientException e) {
            System.out.println("------ Pulsar ERROR ------");
            System.out.println(e.getMessage());
        }
    }

    public static class PulsarRecordsMetric extends DoFn<PulsarMessage, PulsarMessage> {
        private final Counter counter = Metrics.counter(PulsarIOTest.class.getName(), "PulsarRecordsCounter");
        @ProcessElement
        public void processElement(ProcessContext context) {
            counter.inc();
            context.output(context.element());
        }
    }
}
