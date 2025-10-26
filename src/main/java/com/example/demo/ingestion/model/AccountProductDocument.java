package com.example.demo.ingestion.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Getter
@ToString
@NoArgsConstructor
@Document(collection = "account_products")
public class AccountProductDocument {

    private static final String DELIMITER = "|";

    @Id
    private String id;
    private String accountNumber;
    private String productCode;

    public AccountProductDocument(String accountNumber, String productCode) {
        this.id = toId(accountNumber, productCode);
        this.accountNumber = accountNumber;
        this.productCode = productCode;
    }

    public static String toId(String accountNumber, String productCode) {
        return accountNumber + DELIMITER + productCode;
    }
}
