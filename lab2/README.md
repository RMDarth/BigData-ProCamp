# Kafka Lab Work (Lab #2)

This lab work consist of several parts:
 - Kafka configuration
 - NiFi configuration
 - Creating Kafka Consumer 

## Short configuration description

Here is what should be done:
- Execute commands on master VM in Dataproc server to configure Kafka:
```
git clone https://github.com/RMDarth/BigData-ProCamp.git
cd BigData-ProCamp/lab2/
./create_topic.sh
```
- Load NiFi template **`NiFiPublishBtcStockToKafka.xml`**
- Configure and run NiFi processors
- Return to the VM shell to configure and run consumer:
```
sudo apt install librdkafka-dev rapidjson-dev
cd kafka_consumer
make
./kafka_consumer
```
After that consumer should start processing messages from Kafka and displaying top 10 prices and last received price.
More details on each step are described below.

## Kafka configuration

Kafka cluster is up and running on Dataproc server, but we need to create a topic for our messages (with BitCoin deals). There is a shell script to create it. It can be run on any VM on Dataproc server (after cloning current repository and navigating to the lab2 folder):
```
chmod +x create_topic.sh
./create_topic.sh
```
Topic with the name "btc_stock" should be created.

## NiFi configuration

Run NiFi and upload template **`NiFiPublishBtcStockToKafka.xml`**, and apply that template. Several processors should appear. Some additional configuration is needed:
 - In Controller Services, StandardRestrictedSSLContextService properties should be updated, Truststore password is set to "truststore"
 - Both Controller Services should be started
 - Global variable "CurrencyPair" should be added with value "btcusd".

After that, we can run all processor, and messages with bitcoin stock deals should go into Kafka (into "btc_stock" topic)

## Creating Kafka consumer

There is a C++ based Kafka consumer in this repository. It can be built and run on any VM on Dataproc server. For this, navigate into kafka_consumer subfolder (in lab2 folder) and run the following commands:
```
sudo apt install librdkafka-dev rapidjson-dev
make
./kafka_consumer
```
First line install dependencies (kafka dev lib and json lib). After that consumer is built and run. It should start processing messages from "btc_stock" topic, parse them, display the price from current messages and top 10 prices from all messages encountered during this run.
To stop running the consumer press Ctrl+C, it will stop, free kafka consumer connection and display more detailed information about top 10 deals.

