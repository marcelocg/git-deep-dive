package fish.payara.onebrcpayara;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Reader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 1 Billion Row Challenge. See https://1brc.dev/
 *
 * @author Petr
 * @author Fabio
 * @author Rhys
 */
public class OneBRCPayara {

    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
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
            }
            System.out.println("Calculating statistics");
            List<Statistics> results = Collections.synchronizedList(new ArrayList<>());
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
                        results.add(new Statistics(entry.getKey(), min, (sum / entry.getValue().size()), max));
                    });
                }
            }
            System.out.println("Sorting");
            Collections.sort(results, (r1, r2) -> r1.name().compareTo(r2.name()));
            System.out.println("Printing");
            results.stream()
                    .forEach(s -> out.println("%s;%.1f;%.1f;%.1f".formatted(s.name(), s.min(), s.mean(), s.max())));
        }
    }

    private static void addToData(String line, Map<String, List<Double>> stats) throws NumberFormatException {
        int indexOfSemicolon = line.indexOf(';');
        //System.out.println(Arrays.toString(lineParts));
        String name = line.substring(0, indexOfSemicolon);
        Double value = Double.valueOf(line.substring(indexOfSemicolon + 1));
        stats.compute(name, (k, v) -> updatedList(k, v, value));
    }

    private static List<Double> updatedList(String k, List<Double> v, Double value) {
        if (v == null) {
            v = new ArrayList<>(List.of(value));
        }
        v.add(value);
        return v;
    }

    public record Statistics(
            String name,
            double min,
            double mean,
            double max) {

    }
}
