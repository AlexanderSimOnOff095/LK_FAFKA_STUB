package ru.eapo.stub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.fromEnvironment();
        Repository repository = new Repository(config.databaseUrl());
        KafkaWorker worker = new KafkaWorker(
                config.kafkaBootstrapServers(),
                config.kafkaTopic(),
                config.stubConsumerGroup(),
                repository
        );
        AdminServer adminServer = new AdminServer(config.adminPort(), repository);
        Thread workerThread = Thread.ofPlatform().name("kafka-worker").start(worker);

        Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(() -> {
            LOG.info("Stopping LK FAFKA STUB");
            adminServer.close();
            worker.close();
        }));

        adminServer.start();
        workerThread.join();
    }
}
