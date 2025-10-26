package com.example.demo.ingestion.model;

public record JobCreatedResponse(
        String jobId,
        IngestionJobStatus status,
        int fileCount,
        boolean deleteExisting,
        long deletedRecords,
        long processedRecords,
        long totalRecordsEstimate,
        int progressPercent) {

    public static JobCreatedResponse from(IngestionJob job) {
        return new JobCreatedResponse(
                job.getId(),
                job.getStatus(),
                job.getFiles().size(),
                job.isDeleteExisting(),
                job.getDeletedRecords(),
                job.getProcessedRecords(),
                job.getTotalRecordsEstimate(),
                job.getProgressPercent());
    }
}
