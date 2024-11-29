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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Petr Aubrecht <aubrecht@asoftware.cz>
 */
public class OneBRCPayara {

    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
        System.out.println("Let's read the file");
        System.out.println("Available CPU Cores:" + Runtime.getRuntime().availableProcessors());

        Map<String, Statistics> stats = new HashMap<>();
        try (Reader file = new BufferedReader(new FileReader("data/weather_stations.csv"), 1024 * 1024)) {
            Scanner scanner = new Scanner(file);
            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] lineParts = line.split(";");
                System.out.println(Arrays.toString(lineParts));
                if (!stats.containsKey(lineParts[0])) {
                    stats.put(lineParts[0], new ArrayList<>());
                }
                stats.get(lineParts[0]).add(Double.valueOf(lineParts[1]));
            }
            try (ExecutorService es = Executors.newVirtualThreadPerTaskExecutor()) {
                for (Map.Entry<String, List<Double>> entry : stats.entrySet()) {
                    es.submit(() -> {
                        double min = Double.POSITIVE_INFINITY;
                        double max = Double.NEGATIVE_INFINITY;
                        double sum = 0f;
                        for (double f : entry.getValue()) {
                            if (f < min) {
                                min = f;
                            }
                            if (f > max) {
                                max = f;
                            }
                            sum += f;
                        }
                        out.println(new Statistics(entry.getKey(), min, (sum / entry.getValue().size()), max));
                    });
                }
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
