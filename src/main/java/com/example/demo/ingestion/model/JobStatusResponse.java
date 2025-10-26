package com.example.demo.ingestion.model;

import java.time.Instant;
import java.util.List;

public record JobStatusResponse(
        String jobId,
        IngestionJobStatus status,
        Instant createdAt,
        Instant startedAt,
        Instant completedAt,
        long totalRecords,
        long insertedRecords,
        long duplicateRecords,
        long invalidRecords,
        long processedRecords,
        long totalRecordsEstimate,
        int progressPercent,
        long deletedRecords,
        boolean deleteExisting,
        String errorMessage,
        List<IngestionJobFile> files) {

    public static JobStatusResponse from(IngestionJob job) {
        return new JobStatusResponse(
                job.getId(),
                job.getStatus(),
                job.getCreatedAt(),
                job.getStartedAt(),
                job.getCompletedAt(),
                job.getTotalRecords(),
                job.getInsertedRecords(),
                job.getDuplicateRecords(),
                job.getInvalidRecords(),
                job.getProcessedRecords(),
                job.getTotalRecordsEstimate(),
                job.getProgressPercent(),
                job.getDeletedRecords(),
                job.isDeleteExisting(),
                job.getErrorMessage(),
                job.getFiles());
    }
}
