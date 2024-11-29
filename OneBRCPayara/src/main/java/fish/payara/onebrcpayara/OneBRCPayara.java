/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package fish.payara.onebrcpayara;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 *
 * @author Petr Aubrecht <aubrecht@asoftware.cz>
 */
public class OneBRCPayara {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        System.out.println("Let's read the file");

        Map<String, Statistics> stats = new HashMap<>();
        try (Reader file = new BufferedReader(new FileReader("data/weather_stations.csv"), 1024 * 1024)) {
            Scanner scanner = new Scanner(file);
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] lineParts = line.split(";");
                System.out.println(Arrays.toString(lineParts));
            }
        }
    }

    public record Statistics(
            String name,
            double min,
            double mean,
            double max) {

    }
}
