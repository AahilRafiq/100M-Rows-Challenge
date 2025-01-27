
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

public class Solution{
    public static void main(String[] args) {
        try {
            try (RandomAccessFile file = new RandomAccessFile("measurements.txt", "r")) {
                FileChannel channel = file.getChannel();
                MappedByteBuffer buffer = channel.map(MapMode.READ_ONLY, 0, file.length());
                
                while(buffer.limit()  > buffer.position()) {
                    System.out.print((char)buffer.get());
                }
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}