package cluster.management;

public interface OnElectionCallback {
    void onElectedLeader();
    void onWorker();
}
