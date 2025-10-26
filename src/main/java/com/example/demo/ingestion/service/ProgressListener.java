package com.example.demo.ingestion.service;

@FunctionalInterface
interface ProgressListener {
    void onProgress(long processed, long inserted, long duplicates, long invalid);
}
