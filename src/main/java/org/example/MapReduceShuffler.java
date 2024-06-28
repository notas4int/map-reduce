package org.example;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class MapReduceShuffler {
    private final Logger logger = Logger.getLogger(MapReduceWorker.class.getName());
    private static volatile MapReduceShuffler shuffler;
    private final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
    private final Map<String, BufferedWriter> alreadyWrittenTypes = new ConcurrentHashMap<>();

    public static MapReduceShuffler getInstance() {
        if (shuffler == null) {
            synchronized (MapReduceShuffler.class) {
                if (shuffler == null) {
                    shuffler = new MapReduceShuffler();
                }
            }
        }
        return shuffler;
    }

    public void startShuffler(String filePath) {
        logger.info(Thread.currentThread().getName());
        readMappingInfoAndWriteToQueue(filePath);
        shuffle();
    }

    private void readMappingInfoAndWriteToQueue(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            reader.readLine();
            while (reader.ready()) {
                queue.add(reader.readLine());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void shuffle() {
        logger.info("start shuffling");
        BufferedWriter writer;
        while (!queue.isEmpty()) {
            String[] data = queue.poll().split(",");
            Path filePath = Path.of(System.getProperty("user.dir") + "\\src\\main\\java\\org\\resources\\shuffled\\"
                    + data[0] + "_ch_group_items.csv");

            writer = alreadyWrittenTypes.computeIfAbsent(filePath.toString(), k -> {
                try {
                    Files.createFile(filePath);
                    return Files.newBufferedWriter(filePath, StandardOpenOption.APPEND);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            try {
                writer.write(data[0] + "," + data[1] + "\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public Map<String, BufferedWriter> getAlreadyWrittenTypes() {
        return alreadyWrittenTypes;
    }
}
