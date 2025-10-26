package com.example.demo.ingestion.model;

public record JobCreatedResponse(
        String jobId,
        IngestionJobStatus status,
        int fileCount,
        boolean deleteExisting) {

    public static JobCreatedResponse from(IngestionJob job) {
        return new JobCreatedResponse(
                job.getId(),
                job.getStatus(),
                job.getFiles().size(),
                job.isDeleteExisting());
    }
}
