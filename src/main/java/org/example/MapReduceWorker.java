package org.example;


import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class MapReduceWorker {
    private final Logger logger = Logger.getLogger(MapReduceWorker.class.getName());
    private static volatile MapReduceWorker worker;

    public static MapReduceWorker getInstance() {
        if (worker == null) {
            synchronized (MapReduceWorker.class) {
                if (worker == null) {
                    worker = new MapReduceWorker();
                }
            }
        }
        return worker;
    }

    public String startWorker(String filePath) {
        logger.info(Thread.currentThread().getName());
        List<String> mappingInfo = map(filePath);
        try {
            TimeUnit.SECONDS.sleep(6);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return writeToTmpFile(mappingInfo, filePath);
    }

    private List<String> map(String filePath) {
        logger.info("start mapping");
        List<String> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            while (reader.ready()) {
                String[] values = reader.readLine().split(",");
                res.add(values[3] + "," + values[4]);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        System.out.println(res);

        return res;
    }

    private String writeToTmpFile(List<String> info, String filePath) {
        filePath = filePath.substring(filePath.lastIndexOf("\\") + 1, filePath.lastIndexOf(".")) + "_ch" + ".csv";
        filePath = System.getProperty("user.dir") + "\\src\\main\\java\\org\\resources\\mapped\\" + filePath;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (String stringData : info) {
                writer.write(stringData + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        logger.info("ok");
        return filePath;
    }
}
