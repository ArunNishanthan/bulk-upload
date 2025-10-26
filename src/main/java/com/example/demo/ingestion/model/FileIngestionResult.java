package com.example.demo.ingestion.model;

public record FileIngestionResult(
        String filename,
        long totalRecords,
        long insertedRecords,
        long duplicateRecords,
        long invalidRecords,
        long durationMillis) {
}
