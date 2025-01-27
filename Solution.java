
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Solution{
    public static void main(String[] args) {
        Container.initMaps(1);
        try {
            try (RandomAccessFile file = new RandomAccessFile("measurements.txt", "r")) {
                FileChannel channel = file.getChannel();
                MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, 0, file.length());

                var hashMap = Container.maps.get(0);
                byte currByte;
                byte[] cityBytes = new byte[300];
                byte[] tempBytes = new byte[300];
                int cityIdx,tempIdx;
                final byte SEMI_COLON_B = (byte)';';
                final byte NEWLINE_B = (byte)'\n';
                
                while(buffer.position() < buffer.limit()) {
                    cityIdx = 0;
                    tempIdx = 0;
                    while(buffer.position() < buffer.limit() && (currByte = buffer.get()) != SEMI_COLON_B) {
                        cityBytes[cityIdx++] = currByte;
                    }

                    buffer.get(); // Ignore ';''

                    while(buffer.position() < buffer.limit() && (currByte = buffer.get()) != NEWLINE_B) {
                        tempBytes[tempIdx++] = currByte;
                    }

                    if(buffer.position() < buffer.limit()) buffer.get(); // Ignore '\n'

                    Stats.updateStats(
                        new String(cityBytes,0,cityIdx)
                        , Double.parseDouble(new String(tempBytes,0,tempIdx))
                        , hashMap
                    );
                }
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}

class Container{
    public static List<HashMap<String,Stats>> maps;

    public static void initMaps(int num) {
        maps = new ArrayList<>();
        for(int i=0 ; i<num ; i++) {
            maps.add(new HashMap<>());
        }
    }
}

class Stats{
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

    public static void updateStats(String city, double temp, HashMap<String,Stats> hashMap) {
        hashMap.compute(city, (k,stats) -> {
            if(stats == null) stats = new Stats();
            stats.sum += temp;
            if(temp < stats.min) stats.min = temp;
            if(temp > stats.max) stats.max = temp;
            stats.count++;
            return stats;
        });
    }
}