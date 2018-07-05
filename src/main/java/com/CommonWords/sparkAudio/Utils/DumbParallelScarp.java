package com.CommonWords.sparkAudio.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DumbParallelScarp {
    public static final int THREAD_COUNT = 8; // SHOULD BE POWER OF TWO

    public static List<String> parallelGetText(String[] array) {
        Thread[] threads = new Thread[THREAD_COUNT];
        ArrayList<String> list = new ArrayList<>();
        final String[][] chunks = splitOnChunks(array, THREAD_COUNT);

        for (int i = 0; i < THREAD_COUNT; i++) {
            final String[] chunk = chunks[i];

            threads[i] = new Thread(() -> list.addAll(Arrays.stream(chunk).map(n->TextHelper.getText(n)).collect(Collectors.toList())));

            threads[i].start();
        }

        //wait until all threads complete work
        for (int i = 0; i < THREAD_COUNT; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return list;
    }


    //15 threads = 3 3 3 6
    protected static int[] getChunkSize(int length, int chunksCount) {
        int step = length / chunksCount;
        System.err.println(length+"   "+chunksCount+"   "+step);
        int[] sizes = new int[chunksCount];

        for (int i = 0; i < chunksCount - 1; i++) {
            sizes[i] = step;
        }

        sizes[chunksCount - 1] = step + length % chunksCount;

        return sizes;
    }

    protected static String[][] splitOnChunks(String[] array, int chunkCount) {
        int[] sizes = getChunkSize(array.length, chunkCount);
        System.err.println("Chunk size : "+Arrays.toString(sizes));
        String[][] chunks = new String[chunkCount][];

        int start = 0;

        for (int i = 0; i < chunkCount; i++) {
            int end = start + sizes[i];

            chunks[i] = Arrays.copyOfRange(array, start, end);

            start = end;
        }

        return chunks;
    }

}