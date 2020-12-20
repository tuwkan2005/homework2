package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Exchanger;
import java.util.stream.Collectors;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);
    private final Exchanger<List<Pair<String, Integer>>> exchanger;
    private String resultFileName;

    public FileWriter(Exchanger<List<Pair<String, Integer>>> exchanger, @Nonnull String resultFileName) {

        this.exchanger = exchanger;
        this.resultFileName = resultFileName;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());

        final File file = new File(resultFileName);

        if (file.exists()) {
            file.delete();
        }

        try(java.io.FileWriter fileWriter = new java.io.FileWriter(file, true)) {
            while(!Thread.currentThread().isInterrupted()) {
                List<Pair<String, Integer>> pairList = new ArrayList<>();

                pairList = exchanger.exchange(pairList);

                if (pairList.size() == 0) {
                    continue;
                }

                String data = pairList.stream()
                        .map(p -> String.format("%s %d", p.getLeft(), p.getRight()))
                        .collect(Collectors.joining("\n", "", "\n"));

                fileWriter.write(data);
                fileWriter.flush();
            }

        } catch (IOException | InterruptedException exception) {
            logger.error("", exception);
        }

        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
