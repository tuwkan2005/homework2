package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    private static final Exchanger<List<Pair<String, Integer>>> exchanger = new Exchanger<>();
    private final LineCounterProcessor lineCounterProcessor = new LineCounterProcessor();


    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();


    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);

        Thread thread = new Thread(new FileWriter(exchanger, resultFileName));
        thread.start();

        ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {

                List<CompletableFuture<Pair<String, Integer>>> futureList = new ArrayList<>();

                for (int i = 0; i < CHUNK_SIZE; i++) {
                    if (scanner.hasNext()) {
                        String str = scanner.nextLine();

                        CompletableFuture<Pair<String, Integer>> completableFuture = CompletableFuture.supplyAsync(() ->
                                lineCounterProcessor.process(str), executorService);
                        futureList.add(completableFuture);
                    }
                }

                if (futureList.size() > 0) {
                    CompletableFuture <List<Pair<String, Integer>>> future = sequence(futureList);

                    exchanger.exchange(future.get());
                }
            }
        } catch (IOException | InterruptedException | ExecutionException exception) {
            logger.error("", exception);
        }

        thread.interrupt();
        executorService.shutdown();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }

    private <T> CompletableFuture<List<T>> sequence(@Nonnull List<CompletableFuture<T>> futureList) {
        return CompletableFuture.allOf(futureList.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> futureList.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
                );
    }
}
