package com.example.demo.ingestion.support;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.dataformat.univocity.UniVocityCsvDataFormat;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CamelCsvParserFactory extends UniVocityCsvDataFormat {

    public CamelCsvParserFactory() {
        setHeaderExtractionEnabled(false);
        setSkipEmptyLines(true);
        setIgnoreLeadingWhitespaces(true);
        setIgnoreTrailingWhitespaces(true);
        setLineSeparator("\n");
        setLazyLoad(true);
        setAsMap(false);
    }

    public CsvParser newParser() {
        CsvParserSettings settings = createParserSettings();
        configureParserSettings(settings);
        settings.setColumnReorderingEnabled(false);
        settings.setMaxCharsPerColumn(32);
        settings.setIgnoreLeadingWhitespaces(true);
        settings.setIgnoreTrailingWhitespaces(true);
        settings.getFormat().setLineSeparator("\n");
        log.debug("Created CsvParser with maxCharsPerColumn={} lazyLoad={}",
            settings.getMaxCharsPerColumn(), isLazyLoad());
        return createParser(settings);
    }
}
