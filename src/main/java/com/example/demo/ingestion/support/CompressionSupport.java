package com.example.demo.ingestion.support;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CompressionSupport {

    private static final int BUFFER_SIZE = 64 * 1024;

    public InputStream decodeIfNecessary(InputStream original, String filename) throws IOException {
        BufferedInputStream buffered = new BufferedInputStream(original, BUFFER_SIZE);
        if (isGzipFilename(filename)) {
            log.debug("Decompressing gzip file={}", filename);
            return new GzipCompressorInputStream(buffered, true);
        }

        log.debug("Streaming file={} without decompression", filename);
        return buffered;
    }

    private static boolean isGzipFilename(String filename) {
        return filename != null && filename.toLowerCase(Locale.ROOT).endsWith(".gz");
    }
}
