CC=gcc
CXX=g++ -std=c++11 
RM=rm -f
LIBS=-lrdkafka -lm

SRCS=kafka_consumer.cpp HighestPriceMonitor.cpp
DEPS=HighestPriceMonitor.h

all: kafka_consumer

kafka_consumer: $(SRCS) $(DEPS)
	$(CXX) -o kafka_consumer $(SRCS) $(LIBS)
