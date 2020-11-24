// Some parts are based on C kafka consumer example from Confluent
#include <librdkafka/rdkafka.h>
#include <iostream>
#include <csignal>
#include "HighestPriceMonitor.h"

static bool run = true;

void handle_ctrlc (int sig) 
{
    std::cout << "Exiting..." << std::endl;
    run = false;
}

rd_kafka_t* init_kafka_consumer(const char *topic)
{
    auto * conf = rd_kafka_conf_new();
    char errstr[512];

    rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092", nullptr, 0);
    rd_kafka_conf_set(conf, "group.id", "top-10-consumer", nullptr, 0);
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest", nullptr, 0);
    rd_kafka_conf_set(conf, "enable.partition.eof", "false", nullptr, 0);
    rd_kafka_conf_set(conf, "enable.auto.commit", "false", nullptr, 0);

    // create consumer
    auto* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
    if (!rk) 
    {
        std::cerr << "Failed to create consumer: " << errstr << std::endl;
        return nullptr;
    }

    rd_kafka_poll_set_consumer(rk);

    auto* topics = rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(topics, topic,
                                      RD_KAFKA_PARTITION_UA);

    std::cout << "Subscribed to " << topic << std::endl;
    auto err = rd_kafka_subscribe(rk, topics);
    rd_kafka_topic_partition_list_destroy(topics);

    if (err) 
    {
        std::cerr << "Subscribe to " << topic << " failed: " << rd_kafka_err2str(err) << std::endl;
        rd_kafka_destroy(rk);
        return nullptr;
    }

    return rk;
}

int main()
{
    signal(SIGINT, handle_ctrlc);
    signal(SIGTERM, handle_ctrlc);
    auto* rk = init_kafka_consumer("btc_stock");
    if (!rk)
        return -1;

    HighestPriceMonitor price_monitor;

    while (run) 
    {
        auto * rkm = rd_kafka_consumer_poll(rk, 1000);
        if (!rkm)
            continue;

        if (rkm->err) 
        {
            std::cerr << "Consumer error: " << rd_kafka_message_errstr(rkm) << std::endl;
            std::cerr << "Message offset: " << (int)rkm->offset << ", message size: " << (int)rkm->len << std::endl; 
        } else {
            //std::cout << "Received new in topic " << rd_kafka_topic_name(rkm->rkt)
            //          << ", at offset " << (int)rkm->partition << ": " << (const char *)rkm->payload << std::endl;
            price_monitor.ProcessMessage((const char *)rkm->payload);
        }

        rd_kafka_commit_message(rk, rkm, 0);
        rd_kafka_message_destroy(rkm);
        price_monitor.PrintCurrentTop();
    }

    // leave group and close
    rd_kafka_consumer_close(rk);
    rd_kafka_destroy(rk);

    price_monitor.PrintCurrentTopFull();

    return 0;
}