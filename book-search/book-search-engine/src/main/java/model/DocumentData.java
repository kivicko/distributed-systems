package model;

import java.io.Serializable;
import java.util.HashMap;

public class DocumentData implements Serializable {
    private HashMap<String, Double> termToFrequency = new HashMap<>();

    public void putTermFrequency(String term, double frequency) {
        termToFrequency.put(term, frequency);
    }

    public double getFrequency(String term) {
        return termToFrequency.get(term);
    }
}
