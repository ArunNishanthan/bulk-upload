package com.example.demo.ingestion.model;

public record IngestionJobFile(
        String filename,
        IngestionJobStatus status,
        long totalRecords,
        long insertedRecords,
        long duplicateRecords,
        long invalidRecords,
        long durationMillis,
        String errorMessage) {

    public static IngestionJobFile pending(String filename) {
        return new IngestionJobFile(filename, IngestionJobStatus.PENDING, 0, 0, 0, 0, 0, null);
    }

    public static IngestionJobFile completed(FileIngestionResult result) {
        return new IngestionJobFile(
                result.filename(),
                IngestionJobStatus.SUCCEEDED,
                result.totalRecords(),
                result.insertedRecords(),
                result.duplicateRecords(),
                result.invalidRecords(),
                result.durationMillis(),
                null);
    }

    public static IngestionJobFile failed(String filename, String errorMessage) {
        return new IngestionJobFile(
                filename,
                IngestionJobStatus.FAILED,
                0,
                0,
                0,
                0,
                0,
                errorMessage);
    }
}
