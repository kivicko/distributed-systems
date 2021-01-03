package model;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public class Result implements Serializable {
    private Map<String, DocumentData> documentToDocumentData;

    public void addDocumentData(String document, DocumentData documentData) {
        this.documentToDocumentData.put(document, documentData);
    }

    public Map<String, DocumentData> getDocumentToDocumentData() {
        return Collections.unmodifiableMap(documentToDocumentData);
    }
}