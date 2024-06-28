package org.example;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        MapReduceCoordinator coordinator = MapReduceCoordinator.getInstance();
        coordinator.startExtracting();
    }
}
