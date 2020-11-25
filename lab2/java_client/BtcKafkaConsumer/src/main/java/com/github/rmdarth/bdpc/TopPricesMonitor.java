package com.github.rmdarth.bdpc;

import java.util.Iterator;
import java.util.PriorityQueue;
import org.json.*;

public class TopPricesMonitor {
    private class Deal implements Comparable<Deal>
    {
        public String message;
        public String id;
        public double price;

        @Override
        public int compareTo(Deal o) {
            return Double.compare(price, o.price);
        }

        @Override
        public String toString(){
            return Double.toString(price);
        }
    }

    private int maxPrices;
    private PriorityQueue<Deal> topPrices = new PriorityQueue<>();

    public TopPricesMonitor(int maxPrices)
    {
        this.maxPrices = maxPrices;
    }

    public void processMessage(String message)
    {
        try {
            JSONObject obj = new JSONObject(message);
            Deal deal = new Deal();
            deal.message = message;
            deal.id = obj.getJSONObject("data").getString("id_str");
            deal.price = obj.getJSONObject("data").getDouble("price");

            System.out.println("New deal: " + deal.price);

            topPrices.add(deal);
            if (topPrices.size() > maxPrices)
                topPrices.remove();
        } catch (Exception e)
        {
            System.err.println("Error processing message: " + e.getMessage());
        }
    }

    public void printTopPrices()
    {
        System.out.println("Top prices: " + topPrices);
    }

    public void printTopPricesFull()
    {
        System.out.println("Top prices: ");
        for (Deal deal : topPrices)
        {
            // more data could be printed if necessary
            System.out.println("Id: " + deal.id + ", price: " + deal.price);
        }
        System.out.println();
    }
}
