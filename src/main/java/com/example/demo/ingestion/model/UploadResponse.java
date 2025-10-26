package com.example.demo.ingestion.model;

import java.util.List;

public record UploadResponse(
        List<FileIngestionResult> files,
        long totalRecords,
        long totalInserted,
        long totalDuplicates,
        long totalInvalid,
        long durationMillis) {
}
