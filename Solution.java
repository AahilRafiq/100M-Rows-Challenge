
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

public class Solution {
    
    private static HashMap<String,Stats> hashMap;
    
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        final int numThreads = Runtime.getRuntime().availableProcessors()+4;
        Container.initMaps(numThreads);
        
        try {
            Thread[] workers = new Thread[numThreads];
            for(int i=0; i<numThreads; i++) {
                workers[i] = Thread.ofVirtual().start(new Worker(i, numThreads));
            }
            for(Thread t : workers) {
                t.join();
            }
            
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        
        finaliseAns();
        printResult();
        
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) + "ms," + numThreads);
    }

    public static void finaliseAns() {
        var mainMap = Container.maps.get(0);
        Stats currStats, mainStats;
        for(int i=1 ; i<Container.maps.size(); i++) {
            var otherMap = Container.maps.get(i);
            for(Entry<String,Stats> e : otherMap.entrySet()) {
                if(mainMap.containsKey(e.getKey())) {
                    mainStats = mainMap.get(e.getKey());
                    currStats = e.getValue();
                    
                    mainStats.count += currStats.count;
                    mainStats.sum += currStats.sum;
                    if(currStats.min < mainStats.min) mainStats.min = currStats.min;
                    if(currStats.max > mainStats.max) mainStats.max = currStats.max;

                    mainMap.put(e.getKey(),mainStats);

                } else {
                    mainMap.put(e.getKey(),e.getValue());
                }
            }
        }
        hashMap = mainMap;
    }

    public static void printResult() {
        Stats currStats;
        for(Entry<String,Stats> e : hashMap.entrySet()) {
            currStats = e.getValue();
            System.out.printf("Place: %s%nMin: %.2f%nMax: %.2f%nAverage: %.10f%n%n",
                e.getKey(), currStats.min, currStats.max, currStats.getAverage());
        }
    }
}

class Worker implements Runnable {

    private final int threadIdx;
    private final int numThreads;

    Worker(int threadIdx, int numThreads) {
        this.threadIdx = threadIdx;
        this.numThreads = numThreads;
    }

    @Override
    public void run() {
        try (RandomAccessFile file = new RandomAccessFile("measurements.txt", "r")) {
            FileChannel channel = file.getChannel();
            MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, 0, file.length());

            int offset = (int)(file.length()/numThreads)*threadIdx;
            final byte SEMI_COLON_B = (byte) ';';
            final byte NEWLINE_B = (byte) '\n';
            
            // move buffer to offset , and to first line
            buffer.position(offset);
            if(offset != 0) {
                while(buffer.position() < buffer.limit()) {
                    if(buffer.get() == NEWLINE_B) break; 
                } 
            }
            int limit = Math.min(buffer.remaining() , (int)(file.length() / numThreads));

            var hashMap = Container.maps.get(threadIdx);
            byte currByte;
            byte[] cityBytes = new byte[300];
            byte[] tempBytes = new byte[300];
            int cityIdx, tempIdx;
            
            // Note : Limit only checked at while loop
            // Because for case when limit < num of chars in the line , read the whole line
            while (limit > 0) {
                cityIdx = 0;
                tempIdx = 0;
                while (buffer.hasRemaining()) {
                    currByte = buffer.get();
                    limit--;
                    if(currByte == SEMI_COLON_B) break;
                    cityBytes[cityIdx++] = currByte;
                }

                while (buffer.hasRemaining()) {
                    currByte = buffer.get();
                    limit--;
                    if(currByte == NEWLINE_B) break;
                    tempBytes[tempIdx++] = currByte;
                }

                Stats.updateStats(
                    new String(cityBytes, 0, cityIdx),
                    new String(tempBytes, 0, tempIdx),
                    hashMap
                );
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}

class Container {

    public static List<HashMap<String, Stats>> maps;

    public static void initMaps(int num) {
        maps = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            maps.add(new HashMap<>());
        }
    }
}

class Stats {

    double sum;
    double min;
    double max;
    int count;

    Stats() {
        sum = 0;
        min = Double.POSITIVE_INFINITY;
        max = Double.NEGATIVE_INFINITY;
        count = 0;
    }

    public static void updateStats(String city, String tempStr, HashMap<String, Stats> hashMap) {
        if(city.isEmpty()) return;
        Double temp = Double.valueOf(tempStr);
        hashMap.compute(city, (k, stats) -> {
            if (stats == null) {
                stats = new Stats();
            }
            stats.sum += temp;
            if (temp < stats.min) {
                stats.min = temp;
            }
            if (temp > stats.max) {
                stats.max = temp;
            }
            stats.count++;
            return stats;
        });
    }

    public double getAverage() {
        return sum/(double)count;
    }
}
