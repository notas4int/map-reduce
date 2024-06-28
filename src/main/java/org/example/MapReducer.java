package org.example;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

public class MapReducer {
    private final Logger logger = Logger.getLogger(MapReduceWorker.class.getName());
    private static volatile MapReducer reducer;
    private final List<String> res = Collections.synchronizedList(new ArrayList<>());

    public static MapReducer getInstance() {
        if (reducer == null) {
            synchronized (MapReduceShuffler.class) {
                if (reducer == null) {
                    reducer = new MapReducer();
                }
            }
        }
        return reducer;
    }

    public void startReducer(String filePath) {
        logger.info(Thread.currentThread().getName());
        List<String> data = readGroupingInfo(filePath);
        reduce(data);
    }

    private List<String> readGroupingInfo(String filePath) {
        List<String> readyInfo = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            while (reader.ready())
                readyInfo.add(reader.readLine());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return readyInfo;
    }

    private void reduce(List<String> data) {
        int sumPrice = 0;
        String[] dataArr = data.get(0).split(",");
        StringBuilder savingStr = new StringBuilder(dataArr[0]);

        for (String oneDataCell : data) {
            dataArr = oneDataCell.split(",");
            sumPrice += Integer.parseInt(dataArr[1]);
        }
        savingStr.append(",").append(sumPrice);
        res.add(savingStr.toString());
    }

    public String writeReducingInfoToFile() {
        String filePath = System.getProperty("user.dir") + "\\src\\main\\java\\org\\resources\\reduced\\sum_price_of_all_types.csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            for (String str : res) {
                writer.write(str + "\n");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return filePath;
    }
}
