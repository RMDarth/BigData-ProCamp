package com.github.rmdarth.bdpc;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KafkaBtcConsumer {

    public static void main(String[] args) {
        new KafkaBtcConsumer().run();
    }

    private void run()
    {
        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(
                "localhost:9092",
                "top-10-consumer",
                "btc_stock",
                latch
        );
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread( () -> {
            consumerRunnable.shutdown();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public class ConsumerRunnable implements Runnable
    {
        private CountDownLatch latch;
        private KafkaConsumer consumer;

        public ConsumerRunnable(String server, String groupId, String topic, CountDownLatch latch)
        {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer(properties);
            consumer.subscribe(Collections.singleton(topic)); // Arrays.asList("first", "second")

            System.out.println("Subscribed to " + topic);
        }

        @Override
        public void run()
        {
            TopPricesMonitor topPricesMonitor = new TopPricesMonitor(10);

            try {
                while (true)
                {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        // process record
                        topPricesMonitor.processMessage(record.value());
                        topPricesMonitor.printTopPrices();
                    }

                    consumer.commitSync();
                }
            } catch (WakeupException e)
            {
                System.out.println("Received shutdown signal");
            }
            finally {
                consumer.close();
                topPricesMonitor.printTopPricesFull();
                latch.countDown();
            }
        }

        public void shutdown()
        {
            consumer.wakeup();
        }
    }
}