package org.example;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class MapReduceCoordinator {
    private final Logger logger = Logger.getLogger(MapReduceWorker.class.getName());
    private static volatile MapReduceCoordinator mapReduceCoordinator;
    private static MapReduceWorker worker;
    private static MapReduceShuffler shuffler;
    private static MapReducer reducer;

    public static MapReduceCoordinator getInstance() {
        if (mapReduceCoordinator == null) {
            synchronized (MapReduceCoordinator.class) {
                if (mapReduceCoordinator == null) {
                    mapReduceCoordinator = new MapReduceCoordinator();
                    worker = MapReduceWorker.getInstance();
                    shuffler = MapReduceShuffler.getInstance();
                    reducer = MapReducer.getInstance();
                }
            }
        }
        return mapReduceCoordinator;
    }


    public void startExtracting() {
//        int id = LocalDateTime.now().getSecond() + filePath.hashCode() + ThreadLocalRandom.current().nextInt(1, Short.MAX_VALUE);
        String first = "C:\\prog\\map-reduce\\src\\main\\java\\org\\resources\\raw\\east_catalog.csv";
        String second = "C:\\prog\\map-reduce\\src\\main\\java\\org\\resources\\raw\\north_catalog.csv";
        String third = "C:\\prog\\map-reduce\\src\\main\\java\\org\\resources\\raw\\west_catalog.csv";
        String fourth = "C:\\prog\\map-reduce\\src\\main\\java\\org\\resources\\raw\\south_catalog.csv";

        ExecutorService executorService = Executors.newCachedThreadPool();
        FutureTask<String> firstTaskWorker = new FutureTask<>(() -> worker.startWorker(first));
        FutureTask<String> secondTaskWorker = new FutureTask<>(() -> worker.startWorker(second));
        FutureTask<String> thirdTaskWorker = new FutureTask<>(() -> worker.startWorker(third));
        FutureTask<String> fourthTaskWorker = new FutureTask<>(() -> worker.startWorker(fourth));

        executorService.execute(firstTaskWorker);
        executorService.execute(secondTaskWorker);
        executorService.execute(thirdTaskWorker);
        executorService.execute(fourthTaskWorker);

        String firstShuffledFilePath;
        String secondShuffledFilePath;
        String thirdShuffledFilePath;
        String fourthShuffledFilePath;

        try {
            firstShuffledFilePath = firstTaskWorker.get(10, TimeUnit.SECONDS);
            secondShuffledFilePath = secondTaskWorker.get();
            thirdShuffledFilePath = thirdTaskWorker.get();
            fourthShuffledFilePath = fourthTaskWorker.get();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }

        logger.info(firstShuffledFilePath);
        logger.info(secondShuffledFilePath);
        logger.info(thirdShuffledFilePath);
        logger.info(fourthShuffledFilePath);

        executorService.execute(() -> shuffler.startShuffler(firstShuffledFilePath));
        executorService.execute(() -> shuffler.startShuffler(secondShuffledFilePath));
        executorService.execute(() -> shuffler.startShuffler(thirdShuffledFilePath));
        executorService.execute(() -> shuffler.startShuffler(fourthShuffledFilePath));

        try {
            executorService.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        for (Map.Entry<String, BufferedWriter> entry : shuffler.getAlreadyWrittenTypes().entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            executorService.execute(() -> reducer.startReducer(entry.getKey()));
        }
        try {
            executorService.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        logger.info(reducer.writeReducingInfoToFile());
        logger.info("all");
    }
}
