#include "HighestPriceMonitor.h"
#include <iostream>
#include <iomanip>
#include <stdexcept>
#include <cassert>
#define RAPIDJSON_ASSERT(x) if(!(x)) throw std::runtime_error("JSON parsing error")
#define RAPIDJSON_HAS_CXX11_NOEXCEPT 0
#include <rapidjson/document.h>

using namespace rapidjson;

HighestPriceMonitor::HighestPriceMonitor(uint16_t size)
    : _topSize(size)
{
    assert(size > 0);
}

void HighestPriceMonitor::ProcessMessage(const char* jsonMessage)
{
    try 
    {
        Document doc;

        if (doc.Parse(jsonMessage).HasParseError())
        {
            std::cerr << "Can't parse message: " << jsonMessage << std::endl;
            return;
        }

        if (!doc.IsObject() || !doc.HasMember("data") || !doc["data"].IsObject() 
            || !doc["data"].HasMember("price") || !doc["data"].HasMember("id_str"))
        {
            std::cerr << "Invalid message format: " << jsonMessage << std::endl;
            return;
        }

        std::cout << "New deal, price: " << std::setprecision(2) << std::fixed << doc["data"]["price"].GetDouble() << std::endl;

        // We can probably make exactly once guarantee by checking for id uniqueness
        _prices.emplace(
            doc["data"]["id_str"].GetString(),
            jsonMessage,
            doc["data"]["price"].GetDouble());

        if (_prices.size() > _topSize)
        {
            _prices.erase(_prices.begin());
        }
    } 
    catch (std::exception& ex)
    {
        std::cerr << "Error processing message: " << ex.what() << std::endl;
    }
}

void HighestPriceMonitor::PrintCurrentTop()
{
    std::cout << "Top prices: ";
    for (auto& element : _prices)
    {
        std::cout << std::setprecision(2) << std::fixed << element.price;
        if (&element != &(*_prices.rbegin()))
            std::cout << ", ";
    }
    std::cout << std::endl;
}

void HighestPriceMonitor::PrintCurrentTopFull()
{
    std::cout << "Top prices: " << std::endl;
    for (auto& element : _prices)
    {
        // more information could be printed if needed
        std::cout << "Id: " << element.id << ", Price: " << std::setprecision(2) << std::fixed << element.price << std::endl;
    }
    std::cout << std::endl;
}
