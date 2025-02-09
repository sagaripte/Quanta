package com.quanta.examples;

import com.quanta.IndexCardinality;
import com.quanta.Quanta;
import com.quanta.QuantaBuilder;
import com.quanta.Query;
import com.quanta.util.Tuple;
import com.quanta.util.Utils;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

public class NYCTaxiData {

    public static void main(String[] args) throws Exception {
        // Set true to delete & regenerate data
        boolean regenerate_data = true;

        // Set up the NYC taxi data model
        Quanta taxiData = setup(regenerate_data);

        // Populate with simulated taxi records if needed
        if (regenerate_data)
            populateNYCTaxi(taxiData, 20_000_000);

        long startTime = System.currentTimeMillis();

        // Query: Find taxi trips that originated in Manhattan, with Uknown payment
        // and were picked up between 6AM and 11AM (pickup_hour > 5 and < 12).
        Query q = taxiData.newQuery()
                .and("pickup_location", "Manhattan")
                .and("payment_type", "Unknown", "No Charge")
                .gt("pickup_hour", 5)  // pickup hour > 5 (i.e. after 5 AM)
                .lt("pickup_hour", 12) // pickup hour < 12 (i.e. before noon)
                ;

        long duration = System.currentTimeMillis() - startTime;
        System.out.println("Completed search: " + duration + " ms\n");


        // Aggregate metrics over the selected trips
        TaxiStats stats = new TaxiStats();
        startTime = System.currentTimeMillis();
        q.forEach(row -> {
            stats.totalTrips++;
            stats.totalFare += (Double) row.get("fare_amount");
            stats.totalTip += (Double) row.get("tip_amount");
            stats.totalDistance += ((Integer) row.get("trip_distance")) / 100.0;
            stats.totalPassengers += (Integer) row.get("passenger_count");
        });
        duration = System.currentTimeMillis() - startTime;
        System.out.println("Completed looping through results in: " + duration + " ms\n");

        stats.print();
    }

    /**
     * Defines the data model for NYC taxi data.
     */
    public static Quanta setup(boolean cleanup) throws IOException {
        // Data will be stored in the user's home directory under temp/data/nyc_taxi
        String location = Utils.USER_HOME + "/temp/data/nyc_taxi";

        QuantaBuilder qb = new QuantaBuilder("NYCTaxiData", location, cleanup)
                .addIntPrimaryKey("trip_id")
                .addDictionaryColumn("vendor_id", 10)
                .addDictionaryColumn("payment_type", 10)
                .addDictionaryColumn("pickup_location", 20)
                .addDictionaryColumn("dropoff_location", 20)
                .addIntColumn("pickup_year", IndexCardinality.TINY)
                .addIntColumn("pickup_month", IndexCardinality.TINY)
                .addIntColumn("pickup_day", IndexCardinality.TINY)
                .addIntColumn("pickup_hour", IndexCardinality.TINY)
                .addIntColumn("trip_distance", IndexCardinality.MEDIUM)
                .addIntMetric("passenger_count")
                .addMetric("fare_amount")
                .addMetric("tip_amount")
                .addMetric("total_amount");

        return qb.getQuanta();
    }

    /**
     * Populates the Quanta instance with simulated NYC taxi trip data.
     *
     */
    public static void populateNYCTaxi(Quanta quanta, int totalRecords) throws IOException {
        // Sample datasets for randomized fields
        String[] vendorIds = {"B02512", "B02598", "B02764", "CMT", "VTS"};
        String[] paymentTypes = {"Credit Card", "Cash", "No Charge", "Dispute", "Unknown"};
        String[] locations = {"Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"};

        ThreadLocalRandom rand = ThreadLocalRandom.current();
        long start = System.currentTimeMillis();

        for (int i = 0; i < totalRecords; i++) {
            Tuple row = new Tuple();

            row.set("trip_id", i);
            row.set("vendor_id", vendorIds[rand.nextInt(vendorIds.length)]);
            row.set("payment_type", paymentTypes[rand.nextInt(paymentTypes.length)]);
            row.set("pickup_location", locations[rand.nextInt(locations.length)]);
            row.set("dropoff_location", locations[rand.nextInt(locations.length)]);

            // Random pickup date components (year between 2019 and 2024, month 1-12, day 1-28 for simplicity)
            row.set("pickup_year", rand.nextInt(2019, 2025));
            row.set("pickup_month", rand.nextInt(1, 13));
            row.set("pickup_day", rand.nextInt(1, 29));
            // Random pickup hour between 0 and 23
            row.set("pickup_hour", rand.nextInt(0, 24));

            // Passenger count between 1 and 6
            row.set("passenger_count", rand.nextInt(1, 7));

            // Trip distance (in miles) between 0.5 and 50.0
            int tripDistance = (int)(rand.nextDouble(0.5, 50.0) * 100);
            row.set("trip_distance", tripDistance);

            // Fare amount between $3.0 and $100.0
            double fare = rand.nextDouble(3.0, 100.0);
            row.set("fare_amount", fare);

            // Tip amount between $0.0 and $20.0
            double tip = rand.nextDouble(0.0, 20.0);
            row.set("tip_amount", tip);

            // Total amount is fare + tip + a random tax between $0.5 and $5.0
            double tax = rand.nextDouble(0.5, 5.0);
            row.set("total_amount", fare + tip + tax);

            quanta.add(row);

            if (i % 1_000_000 == 0)
                System.out.println("Added: " + i);
        }

        quanta.rebuild();
        System.out.println("Populated " + totalRecords + " NYC taxi records in "
                + (System.currentTimeMillis() - start) + " ms\n");
    }

    /**
     * A simple struct to aggregate taxi trip statistics.
     */
    static class TaxiStats {
        int totalTrips = 0;
        double totalFare = 0.0;
        double totalTip = 0.0;
        double totalDistance = 0.0;
        int totalPassengers = 0;

        void print() {
            System.out.println("NYC Taxi Data Aggregated Stats:");
            System.out.println("  Total Trips: " + totalTrips);
            System.out.println("  Total Fare: $" + totalFare);
            System.out.println("  Total Tip: $" + totalTip);
            System.out.println("  Total Distance: " + totalDistance + " miles");
            System.out.println("  Total Passengers: " + totalPassengers);
            if (totalTrips > 0) {
                System.out.println("  - Average Fare per Trip: $" + (totalFare / totalTrips));
                System.out.println("  - Average Tip per Trip: $" + (totalTip / totalTrips));
                System.out.println("  - Average Distance per Trip: " + (totalDistance / totalTrips) + " miles");
                System.out.println("  - Average Passengers per Trip: " + ((double) totalPassengers / totalTrips));
            }
        }
    }
}
