package com.quanta.examples;

import com.quanta.IndexCardinality;
import com.quanta.Quanta;
import com.quanta.QuantaBuilder;
import com.quanta.Query;
import com.quanta.util.Tuple;
import com.quanta.util.Utils;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class AdEvents {

    public static void main(String[] args) throws Exception {
        boolean delete_old_data = true; // Set true to delete & regenerate data

        // setup data model
        Quanta adEvents = setup(delete_old_data);

        // Insert millions of ad events in real-time
        if (delete_old_data)
            populateAdEvents(adEvents, 150_000_000);

        long time = System.currentTimeMillis();

        // Query: Find high-performing ads in 'North America' on 'Mobile' & 'Tablet'
        Query q = adEvents.newQuery()
                .and("region", "North America")
                .and("device", "Mobile", "Tablet")
                .gt("year", 2021) // Year > 2021
                .not("month", 5, 6) // Exclude May & June
                .lt("day", 15); // Day < 15

        time = System.currentTimeMillis() - time;
        System.out.println("Finished searching records in: " + time + "ms\n");

        // Struct to store aggregated data
        class EventsData {
            int impressions;
            int clicks;
            double bid_price;
            int rows;

            void print(String name) {
                System.out.println("Ad events data for: " + name);
                System.out.println("  Impressions: " + impressions);
                System.out.println("  Clicks: " + clicks);
                System.out.println("  Avg Bid Price: " + (bid_price / rows));
                System.out.println("  # of Rows: " + rows);
                System.out.println("\n");
            }
        }

        EventsData mobileData = new EventsData();
        EventsData tableData = new EventsData();

        q.forEach(row -> {
            EventsData data = row.get("device").equals("Mobile") ? mobileData : tableData;

            data.impressions += (Integer)row.get("impressions");
            data.clicks += (Integer)row.get("clicks");
            data.bid_price += (Double)row.get("bid_price");
            data.rows++;
        });


        mobileData.print("Mobile");
        tableData.print("Tablet");
    }

    public static Quanta setup(boolean cleanup) throws IOException {
        String location = Utils.USER_HOME + "/temp/data/ad_tracking";

        QuantaBuilder qb = new QuantaBuilder("AdEvents", location, cleanup)
                .addIntPrimaryKey("event_id")
                .addStringColumn("campaign", 30, IndexCardinality.TINY)
                .addDictionaryColumn("region", 10)
                .addDictionaryColumn("device", 10)
                .addIntColumn("year", IndexCardinality.TINY)
                .addIntColumn("month", IndexCardinality.TINY)
                .addIntColumn("day", IndexCardinality.TINY)
                .addIntMetric("impressions")
                .addIntMetric("clicks")
                .addMetric("bid_price");

        return qb.getQuanta();

    }

    public static void populateAdEvents(Quanta quanta, int totalRecords) throws IOException {
        // Sample data sets
        String[] campaigns = {"SummerSale", "BlackFriday", "HolidayDeals", "TechExpo", "GamingFest"};
        String[] regions = {"North America", "Europe", "Asia", "South America", "Australia"};
        String[] devices = {"Mobile", "Desktop", "Tablet", "SmartTV"};

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        long start = System.currentTimeMillis();

        for (int i = 0; i < totalRecords; i++) {
            Tuple row = new Tuple();

            row.set("event_id", i);
            row.set("campaign", campaigns[rand.nextInt(campaigns.length)]);
            row.set("region", regions[rand.nextInt(regions.length)]);
            row.set("device", devices[rand.nextInt(devices.length)]);
            row.set("year", rand.nextInt(2020, 2026));
            row.set("month", rand.nextInt(1, 13));
            row.set("day", rand.nextInt(1, 32));
            row.set("impressions", rand.nextInt(1, 1000));
            row.set("clicks", rand.nextInt(0, 500)); // Clicks cannot exceed impressions
            row.set("bid_price", rand.nextDouble(0.1, 5.0)); // Simulating CPM bids

            quanta.add(row);

            if (i % 1_000_000 == 0)
                System.out.println("Added : " + i);
        }

        quanta.rebuild();

        System.out.println("Populated " + totalRecords + " ad events in " + (System.currentTimeMillis() - start) + " ms\n");
    }
}
