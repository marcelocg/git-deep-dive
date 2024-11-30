package fish.payara.onebrcpayara;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
Results:
111,772,193 lines
real	0m5.618s
user	0m56.874s
sys	0m3.507s

1,117,319,693 lines
real	0m48.576s
user	8m51.989s
sys	0m6.459s
 */
/**
 * 1 Billion Row Challenge. See https://1brc.dev/
 *
 * @author Petr
 * @author Fabio
 * @author Rhys
 */
public class OneBRCPayaraParallelRead {

    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
        int cores = Runtime.getRuntime().availableProcessors();
        System.out.println("Available CPU Cores:" + cores);

        Map<String, Statistics> finalStats = new ConcurrentHashMap<>();
        System.out.println("Let's read the file");
        File inFile = new File("../weather_stations.csv");
        long fileSize = inFile.length();
        long chunkSize = fileSize / cores;
        try (ExecutorService es = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int chunkI = 0; chunkI < cores; chunkI++) {
                final int chunk = chunkI;
                es.submit(() -> {
                    byte tmpBuffer[] = new byte[50];
                    Map<String, Statistics> stats = new HashMap<>();
                    long chunkStart = chunk * chunkSize;
                    long start = chunkStart;
                    try (RandomAccessFile chunkFile = new RandomAccessFile(inFile, "r")) {
                        // find the real start
                        if (start == 0) {
                            // skip 2 title lines
                            int linesToSkip = 2;
                            while (linesToSkip > 0) {
                                // decrement lines at the end of line
                                linesToSkip -= chunkFile.readByte() == '\n' ? 1 : 0;
                            }
                        } else {
                            // find the next new line
                            chunkFile.seek(start);
                            while (chunkFile.readByte() != '\n') {
                                // until end of line
                            }
                        }
                        start = chunkFile.getFilePointer();

                        long length;
                        if (chunkStart + chunkSize < fileSize) {
                            chunkFile.seek(chunkStart + chunkSize);
                            while (chunkFile.readByte() != '\n') {
                                // until end of line
                            }
                            length = chunkFile.getFilePointer() - start;
                        } else {
                            length = fileSize - start;
                        }

                        //System.out.println("Chunk #%d %,d - %,d, len %,d%n".formatted(chunk, start, start + length, length));
                        // load the buffer
                        MappedByteBuffer byteBuffer = chunkFile.getChannel().map(MapMode.READ_ONLY, start, length);
                        byteBuffer.load();

                        // process the buffer
                        while (byteBuffer.hasRemaining()) {
                            char c = 0;
                            // name
                            int nameStart = byteBuffer.position();
                            int nameLen = 0;
                            while (c != ';') {
                                c = (char) byteBuffer.get();
                                if (c != ';') {
                                    tmpBuffer[nameLen] = (byte) c;
                                    nameLen++;
                                }
                            }
                            String name = new String(tmpBuffer, 0, nameLen);
                            // value
                            StringBuilder valueBuffer = new StringBuilder();
                            while (c != '\n') {
                                c = (char) byteBuffer.get();
                                if (c != '\n') {
                                    valueBuffer.append(c);
                                }
                            }
                            Double value = Double.valueOf(valueBuffer.toString());
                            stats.compute(name, (k, v) -> updatedStats(k, v, value));
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Do not do it at home!", e);
                    }
                    for (Map.Entry<String, Statistics> entry : stats.entrySet()) {
                        String name = entry.getKey();
                        Statistics stat = entry.getValue();
                        finalStats.compute(name, (k, v) -> updatedStats(k, v, stat));
                    }
                });
            }
        }
        System.out.println("Sorting, print");
        try (PrintStream out = new PrintStream(new FileOutputStream("stats.csv"))) {
            SortedMap<String, Statistics> sortedMap = new TreeMap<>(finalStats);
            System.out.println("Sorted, printing");
            sortedMap.entrySet()
                    .stream()
                    .forEachOrdered(e -> out.println(e.getValue().result()));
            System.out.println("done");
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

    private static Statistics updatedStats(String name, Statistics stat, double value) {
        if (stat == null) {
            return new Statistics(name, value, 1, value, value);
        } else {
            return new Statistics(name, Math.min(stat.min(), value), stat.count() + 1, stat.sum() + value, Math.max(stat.max(), value));
        }
    }

    private static Statistics updatedStats(String name, Statistics stat1, Statistics stat2) {
        if (stat1 == null) {
            return stat2;
        } else {
            return new Statistics(name, Math.min(stat1.min(), stat2.min()), stat1.count() + stat2.count(), stat1.sum() + stat2.sum(), Math.max(stat1.max(), stat2.max()));
        }
    }

    public record Statistics(
            String name, // TODO: can be removed
            double min,
            int count,
            double sum,
            double max) {

        public String result() {
            return "%s;%.1f;%.1f;%.1f".formatted(name, min, sum / count, max);
        }

    }
}
