package search;

import com.google.protobuf.InvalidProtocolBufferException;
import management.ServiceRegistry;
import model.DocumentData;
import model.Result;
import model.SerializationUtils;
import model.Task;
import model.proto.SearchModel;
import networking.OnRequestCallback;
import networking.WebClient;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class SearchCoordinator implements OnRequestCallback {
    public static final String ENDPOINT = "/search";
    public static final String BOOKS_DIRECTORY = "./resources/books/";
    private ServiceRegistry workersServiceRegistry;
    private WebClient client;
    private List<String> documents;

    public SearchCoordinator(ServiceRegistry workersServiceRegistry, WebClient webClient) {
        this.workersServiceRegistry = workersServiceRegistry;
        this.client = webClient;
        this.documents = readDocumentsList();

    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        try {
            SearchModel.Request request = SearchModel.Request.parseFrom(requestPayload);
            SearchModel.Response response = createResponse(request);

            return response.toByteArray();
        } catch (InvalidProtocolBufferException | InterruptedException | KeeperException e) {
            e.printStackTrace();
            return SearchModel.Response.getDefaultInstance().toByteArray();
        }
    }

    private SearchModel.Response createResponse(SearchModel.Request request) throws KeeperException, InterruptedException {
        SearchModel.Response.Builder searchResponse = SearchModel.Response.newBuilder();

        System.out.println("received search query: " + request.getSearchQuery());

        List<String> searchTerms = TFIDF.getWordsFromLine(request.getSearchQuery());

        List<String> workers = workersServiceRegistry.getAllServiceAddresses();

        if(workers.isEmpty()) {
            System.out.println("No search workers currently available");
            return searchResponse.build();
        }

        List<Task> tasks = createTasks(workers.size(), searchTerms);
        List<Result> results = sendTasksToWorkers(workers, tasks);


        List<SearchModel.Response.DocumentStats> sortedDocuments = aggregateResults(results, searchTerms);
        searchResponse.addAllRelevantDocuments(sortedDocuments);
        return searchResponse.build();

    }

    private List<SearchModel.Response.DocumentStats> aggregateResults(List<Result> results, List<String> searchTerms) {
        Map<String, DocumentData> allDocumentsResults = new HashMap<>();

        for(Result result : results) {
            allDocumentsResults.putAll(result.getDocumentToDocumentData());
        }

        System.out.println("Calculating score for all the documents");

        Map<Double, List<String>> scoreToDocuments = TFIDF.getDocumentsSortedByScore(searchTerms, allDocumentsResults);

        //todo garip oldu
        return sortDocumentsByScore(scoreToDocuments);
    }

    private List<SearchModel.Response.DocumentStats> sortDocumentsByScore(Map<Double, List<String>> documentsSortedByScore) {
        List<SearchModel.Response.DocumentStats> result = new ArrayList<>();

        for(Map.Entry<Double, List<String>> docScorePair : documentsSortedByScore.entrySet()) {
            double score = docScorePair.getKey();

            for(String document : docScorePair.getValue()) {
                File documentPath = new File(document);

                SearchModel.Response.DocumentStats documentStats = SearchModel.Response.DocumentStats.newBuilder()
                        .setScore(score)
                        .setDocumentName(documentPath.getName())
                        .setDocumentSize(documentPath.length())
                        .build();

                result.add(documentStats);
            }
        }

        return result;
    }

    @Override
    public String getEndpoint() {
        return ENDPOINT;
    }

    private List<Result> sendTasksToWorkers(List<String> workers, List<Task> tasks) {
        CompletableFuture<Result> [] futures = new CompletableFuture[workers.size()];
        for(int i = 0; i < workers.size(); i++) {
            String worker = workers.get(i);
            Task task = tasks.get(i);

            byte[] serializedTask = SerializationUtils.serialize(task);
            futures[i] = client.sendTask(worker, serializedTask);
        }

        List<Result> results = new ArrayList<>();
        for(CompletableFuture<Result> future : futures) {
            try {
                Result result = future.get();
                results.add(result);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        System.out.printf("Received %d/%d results%n", results.size(), tasks.size());
        return results;
    }

    public List<Task> createTasks(int numberOfWorkers, List<String> searchTerms) {
        List<List<String>> workersDocuments = splitDocumentList(numberOfWorkers, documents);

        List<Task> tasks = new ArrayList<>();
        for(List<String> documentsForWorker : workersDocuments) {
            Task task = new Task(searchTerms, documentsForWorker);
            tasks.add(task);
        }

        return tasks;
    }

    private static List<List<String>> splitDocumentList(int numberOfWorkers, List<String> documents) {
        int numberOfDocumentsPerWorker = (documents.size() + numberOfWorkers - 1) / numberOfWorkers;

        List<List<String>> workersDocuments = new ArrayList<>();

        for (int i = 0; i < numberOfDocumentsPerWorker; i++) {
            int firstDocumentIndex = i * numberOfDocumentsPerWorker;
            int lastDocumentIndexExclusive = Math.min(firstDocumentIndex + numberOfDocumentsPerWorker, documents.size());

            if(firstDocumentIndex >= lastDocumentIndexExclusive) {
                break;
            }

            List<String> currentWorkerDocuments = new ArrayList<>(documents.subList(firstDocumentIndex, lastDocumentIndexExclusive));

            workersDocuments.add(currentWorkerDocuments);
        }
        return workersDocuments;
    }

    private static List<String> readDocumentsList() {
        File documentsDirectory = new File(BOOKS_DIRECTORY);
        return Arrays.stream(Objects.requireNonNull(documentsDirectory.list()))
                .map(documentName -> BOOKS_DIRECTORY + "/" + documentName)
                .collect(Collectors.toList());
    }
}
