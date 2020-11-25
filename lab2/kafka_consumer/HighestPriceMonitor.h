#pragma once
#include <set>
#include <string>

class HighestPriceMonitor
{
public:
    HighestPriceMonitor(uint16_t size = 10);
    void ProcessMessage(const char* jsonMessage);
    void PrintCurrentTop();
    void PrintCurrentTopFull();

private:
    struct Item
    {
        Item(std::string messageId, std::string message, double messagePrice) : id(messageId), fullMessage(message), price(messagePrice) {}

        bool operator<(const Item& other) const
        {
            return price < other.price;
        }

        std::string id;
        std::string fullMessage;
        double price;
    };

    uint16_t _topSize;
    std::multiset<Item> _prices;

};