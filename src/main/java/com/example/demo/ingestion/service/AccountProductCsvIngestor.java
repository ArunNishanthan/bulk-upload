package com.example.demo.ingestion.service;

import com.example.demo.ingestion.model.AccountProductDocument;
import com.example.demo.ingestion.model.FileIngestionResult;
import com.example.demo.ingestion.support.CamelCsvParserFactory;
import com.example.demo.ingestion.support.CompressionSupport;
import com.example.demo.ingestion.support.FileProcessingException;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.univocity.parsers.csv.CsvParser;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

@Slf4j
class AccountProductCsvIngestor {

    private static final int DUPLICATE_KEY_ERROR_CODE = 11000;

    private final CamelCsvParserFactory parserFactory;
    private final CompressionSupport compressionSupport;
    private final MongoCollection<Document> collection;
    private final BulkWriteOptions writeOptions;
    private final int batchSize;
    private final long progressUpdateInterval;

    AccountProductCsvIngestor(CamelCsvParserFactory parserFactory,
            CompressionSupport compressionSupport,
            MongoCollection<Document> collection,
            BulkWriteOptions writeOptions,
            int batchSize,
            long progressUpdateInterval) {
        this.parserFactory = parserFactory;
        this.compressionSupport = compressionSupport;
        this.collection = collection;
        this.writeOptions = writeOptions;
        this.batchSize = batchSize;
        this.progressUpdateInterval = progressUpdateInterval;
    }

    FileIngestionResult ingest(String filename, InputStream rawStream, ProgressListener listener) {
        Instant start = Instant.now();
        CsvParser parser = parserFactory.newParser();
        FileIngestionAccumulator accumulator = new FileIngestionAccumulator(filename, listener, progressUpdateInterval);

        try (InputStream decoded = compressionSupport.decodeIfNecessary(rawStream, filename);
                Reader reader = new InputStreamReader(new BufferedInputStream(decoded), StandardCharsets.UTF_8)) {
            parseRows(parser, reader, accumulator);
        } catch (IOException ex) {
            throw new FileProcessingException("Failed to process CSV for file %s".formatted(filename), ex);
        }

        FileIngestionResult result = accumulator.toResult(Duration.between(start, Instant.now()).toMillis());
        log.info("Completed ingestion for file={} processed={} inserted={} duplicates={} invalid={} durationMs={}",
                result.filename(), result.totalRecords(), result.insertedRecords(), result.duplicateRecords(),
                result.invalidRecords(), result.durationMillis());
        return result;
    }

    long estimateRecordCount(Path file, String filename) {
        try (InputStream raw = Files.newInputStream(file);
                InputStream decoded = compressionSupport.decodeIfNecessary(raw, filename);
                BufferedReader reader = new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8))) {
            boolean headerSkipped = false;
            long count = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }
                count++;
            }
            return count;
        } catch (IOException ex) {
            log.warn("Unable to estimate total records for {}: {}", filename, ex.getMessage());
            return -1;
        }
    }

    private void parseRows(CsvParser parser, Reader reader, FileIngestionAccumulator accumulator) throws IOException {
        parser.beginParsing(reader);
        List<WriteModel<Document>> batch = new ArrayList<>(batchSize);
        boolean headerSkipped = false;

        try {
            String[] row;
            while ((row = parser.parseNext()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }

                accumulator.incrementProcessed();
                Optional<WriteModel<Document>> writeModel = toWriteModel(row);
                if (writeModel.isEmpty()) {
                    accumulator.incrementInvalid();
                    continue;
                }

                batch.add(writeModel.get());
                if (batch.size() >= batchSize) {
                    accumulator.recordWrite(flushBatch(batch));
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                accumulator.recordWrite(flushBatch(batch));
            }
        } finally {
            parser.stopParsing();
        }
    }

    private BulkWriteSummary flushBatch(List<WriteModel<Document>> batch) {
        if (batch.isEmpty()) {
            return BulkWriteSummary.EMPTY;
        }

        try {
            BulkWriteResult result = collection.bulkWrite(batch, writeOptions);
            return new BulkWriteSummary(result.getInsertedCount(), 0);
        } catch (MongoBulkWriteException ex) {
            long duplicates = ex.getWriteErrors().stream()
                    .filter(error -> error.getCode() == DUPLICATE_KEY_ERROR_CODE)
                    .count();
            long inserted = ex.getWriteResult() != null ? ex.getWriteResult().getInsertedCount() : 0;
            long otherErrors = ex.getWriteErrors().stream()
                    .map(BulkWriteError::getCode)
                    .filter(code -> code != DUPLICATE_KEY_ERROR_CODE)
                    .count();

            if (otherErrors > 0) {
                throw new FileProcessingException("Bulk write failed with non-duplicate errors", ex);
            }
            return new BulkWriteSummary(inserted, duplicates);
        }
    }

    private Optional<WriteModel<Document>> toWriteModel(String[] row) {
        if (row == null || row.length < 2) {
            return Optional.empty();
        }
        String accountNumber = safeTrim(row[0]);
        String productCode = safeTrim(row[1]);
        if (!isValidAccountNumber(accountNumber) || !isValidProductCode(productCode)) {
            return Optional.empty();
        }
        return Optional.of(new InsertOneModel<>(toBsonDocument(accountNumber, productCode)));
    }

    private static Document toBsonDocument(String accountNumber, String productCode) {
        Document doc = new Document();
        doc.put("_id", AccountProductDocument.toId(accountNumber, productCode));
        doc.put("accountNumber", accountNumber);
        doc.put("productCode", productCode);
        return doc;
    }

    private static String safeTrim(String value) {
        return value == null ? "" : value.trim();
    }

    private static boolean isValidAccountNumber(String accountNumber) {
        return !accountNumber.isEmpty() && accountNumber.length() <= 15;
    }

    private static boolean isValidProductCode(String productCode) {
        return !productCode.isEmpty() && productCode.length() <= 4;
    }

    private record BulkWriteSummary(long inserted, long duplicates) {
        private static final BulkWriteSummary EMPTY = new BulkWriteSummary(0, 0);
    }

    private static final class FileIngestionAccumulator {
        private final String filename;
        private final ProgressListener listener;
        private final long interval;

        private long nextReportThreshold;
        private long processed;
        private long inserted;
        private long duplicates;
        private long invalid;

        private FileIngestionAccumulator(String filename, ProgressListener listener, long interval) {
            this.filename = filename;
            this.listener = listener;
            this.interval = interval;
            this.nextReportThreshold = interval;
        }

        void incrementProcessed() {
            processed++;
            maybeReport(false);
        }

        void incrementInvalid() {
            invalid++;
        }

        void recordWrite(BulkWriteSummary summary) {
            inserted += summary.inserted();
            duplicates += summary.duplicates();
            maybeReport(false);
        }

        FileIngestionResult toResult(long durationMillis) {
            maybeReport(true);
            return new FileIngestionResult(filename, processed, inserted, duplicates, invalid, durationMillis);
        }

        private void maybeReport(boolean force) {
            if (listener == null) {
                return;
            }
            if (force || processed >= nextReportThreshold) {
                listener.onProgress(processed, inserted, duplicates, invalid);
                nextReportThreshold = processed + interval;
            }
        }
    }
}
