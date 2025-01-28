
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Solution {

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        final int numThreads = 1;
        Container.initMaps(numThreads);

        try {
            Thread t = Thread.ofVirtual().unstarted(new Worker(0,numThreads));
            t.start();
            t.join();
            
        } catch (InterruptedException e) {
        }

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) + "ms");
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
            buffer.get(); // Skip Newline
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

                if (buffer.hasRemaining()) {
                    buffer.get(); // Ignore ';'
                    limit--;
                }

                while (buffer.hasRemaining()) {
                    currByte = buffer.get();
                    limit--;
                    if(currByte == NEWLINE_B) break;
                    tempBytes[tempIdx++] = currByte;
                }

                if (buffer.hasRemaining()) {
                    buffer.get(); // Ignore '\n'
                    limit--;
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
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
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
}
