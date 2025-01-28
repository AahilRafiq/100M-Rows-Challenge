import java.io.*;
import java.util.*;


public class Main {
    public static void main(String[] args) {
        Map<String, Stats> data = new HashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader("measurements.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split(";");
                if (parts.length == 2) {
                    String place = parts[0].trim();
                    try {
                        double measurement = Double.parseDouble(parts[1].trim());
                        data.putIfAbsent(place, new Stats());
                        data.get(place).update(measurement);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid measurement: " + parts[1]);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error reading the file: " + e.getMessage());
            return;
        }
        
        for (Map.Entry<String, Stats> entry : data.entrySet()) {
            String place = entry.getKey();
            Stats stats = entry.getValue();
            System.out.printf("Place: %s%nMin: %.2f%nMax: %.2f%nAverage: %.10f%n%n",
            place, stats.min, stats.max, stats.getAverage());
        }
    }
}

class Stats {
    double min;
    double max;
    double sum;
    int count;

    public Stats() {
        this.min = Double.MAX_VALUE;
        this.max = Double.MIN_VALUE;
        this.sum = 0.0;
        this.count = 0;
    }

    public void update(double measurement) {
        if (measurement < min) min = measurement;
        if (measurement > max) max = measurement;
        sum += measurement;
        count++;
    }

    public double getAverage() {
        return sum / count;
    }
}