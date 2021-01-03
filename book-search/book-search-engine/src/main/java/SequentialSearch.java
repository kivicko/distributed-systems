import model.DocumentData;
import search.TFIDF;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class SequentialSearch {
    public static final String BOOKS_DIRECTORY = "./resources/books";
    public static final String SEARCH_QUERY_1 = "the best detective that catches many criminals using his deductive methods";
    public static final String SEARCH_QUERY_2 = "The girl that falls through a rabbit hole into a fantasy wonderland";
    public static final String SEARCH_QUERY_3 = "A war between Russia and France in the cold winter";

    //it is not necessary anymore.
    public static void main123(String[] args) throws FileNotFoundException {
        File documentsDirectory = new File(BOOKS_DIRECTORY);

        List<String> documents = Arrays.asList(documentsDirectory.list())
                .stream()
                .map(documentName -> BOOKS_DIRECTORY + "/" + documentName)
                .collect(Collectors.toList());

        List<String> terms = TFIDF.getWordsFromLine(SEARCH_QUERY_3);

        findMostRelevantDocuments(documents, terms);
    }

    private static void findMostRelevantDocuments(List<String> documents, List<String> terms) throws FileNotFoundException {
        Map<String, DocumentData> documentDataMap = new HashMap<>();

        for(String document : documents) {
            BufferedReader reader = new BufferedReader(new FileReader(document));
            List<String> lines = reader.lines().collect(Collectors.toList());
            List<String> words = TFIDF.getWordsFromLines(lines);
            DocumentData documentData = TFIDF.createDocumentData(words, terms);
            documentDataMap.put(document, documentData);
        }

        Map<Double, List<String>> documentsSortedByScore = TFIDF.getDocumentsSortedByScore(terms, documentDataMap);
        printResults(documentsSortedByScore);

    }

    private static void printResults(Map<Double, List<String>> documentsSortedByScore) {
        for(Map.Entry<Double, List<String>> docScorePair : documentsSortedByScore.entrySet()) {
            double score = docScorePair.getKey();
            for(String document : docScorePair.getValue()) {
                System.out.println(String.format("Book : %s - score : %f", document.split("/")[3], score));
            }
        }
    }
}