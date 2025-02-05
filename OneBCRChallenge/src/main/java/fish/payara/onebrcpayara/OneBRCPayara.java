package fish.payara.onebrcpayara;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class OneBRCPayara {

    public static void main(String[] args) throws IOException {
        System.out.println("Let's read the file");

        Map<String, Statistic> stats = new HashMap<>();
        try(Reader file = new BufferedReader(new FileReader("data/weather_stations.csv"), 1024 *1024)) {
            final Scanner scanner = new Scanner(file);
            while(scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] lineParts = line.split(";");
                System.out.println(Arrays.toString(lineParts));

            }
        }
    }

    public record Statistic(String name, double min, double mean, double max) {

    }
}
