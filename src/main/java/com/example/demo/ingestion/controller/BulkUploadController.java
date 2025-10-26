package com.example.demo.ingestion.controller;

import com.example.demo.ingestion.model.IngestionJob;
import com.example.demo.ingestion.model.JobCreatedResponse;
import com.example.demo.ingestion.model.JobStatusResponse;
import com.example.demo.ingestion.service.BulkUploadService;
import com.example.demo.ingestion.support.FileProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class BulkUploadController {

    private final BulkUploadService bulkUploadService;

    @PostMapping("/ingestions")
    public ResponseEntity<JobCreatedResponse> upload(@RequestParam("files") MultipartFile[] files,
                                                     @RequestParam(value = "deleteExisting", defaultValue = "false")
                                                     boolean deleteExisting) {
        IngestionJob job = bulkUploadService.enqueue(files, deleteExisting);
        log.info("Accepted ingestion job={} fileCount={}", job.getId(), job.getFiles().size());

        return ResponseEntity.accepted()
            .location(ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{jobId}")
                .buildAndExpand(job.getId())
                .toUri())
            .body(JobCreatedResponse.from(job));
    }

    @GetMapping("/ingestions/{jobId}")
    public ResponseEntity<JobStatusResponse> getStatus(@PathVariable String jobId) {
        IngestionJob job = bulkUploadService.findJob(jobId);
        if (job == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(JobStatusResponse.from(job));
    }

    @PostMapping("/ingestions/{jobId}/reset")
    public ResponseEntity<JobStatusResponse> resetJob(@PathVariable String jobId) {
        IngestionJob job = bulkUploadService.resetJob(jobId);
        if (job == null) {
            return ResponseEntity.notFound().build();
        }
        log.info("Reset ingestion job={} to PENDING state", jobId);
        return ResponseEntity.ok(JobStatusResponse.from(job));
    }


    @GetMapping("/account-products/export")
    public ResponseEntity<StreamingResponseBody> exportAccountProducts(
            @RequestParam(value = "filename", defaultValue = "account-products.csv") String filename) {
        StreamingResponseBody body = outputStream -> bulkUploadService.exportAccountProducts(outputStream);
        return ResponseEntity.ok()
            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=" + filename)
            .contentType(MediaType.parseMediaType("text/csv"))
            .body(body);
    }

    @ExceptionHandler(FileProcessingException.class)
    public ResponseEntity<String> handleFileProcessingException(FileProcessingException exception) {
        log.warn("Ingestion request rejected: {}", exception.getMessage());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(exception.getMessage());
    }
}
