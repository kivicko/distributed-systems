package cluster.management;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZnode;
    private List<String> allServiceAddresses;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata) {
        if (this.currentZnode != null) {
            System.out.println("Already registered to service registry");
            return;
        }

        try {
            this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            System.out.println("registered to service registry.");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void registerForUpdates() {
        updateAddresses();
    }

    public synchronized List<String> getAllServiceAddresses() {
        if (allServiceAddresses == null) {
            updateAddresses();
        }

        return allServiceAddresses;
    }

    public void unregisterFromCluster() throws KeeperException, InterruptedException {
        if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
            zooKeeper.delete(currentZnode, -1);
        }
    }


    private void createServiceRegistryZnode() {
        try {
            if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) {
                zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private synchronized void updateAddresses() {
        try {
            List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);

            List<String> addresses = new ArrayList<>(workerZnodes.size());

            for (String workerZnode : workerZnodes) {
                String workerFullPath = REGISTRY_ZNODE + "/" + workerZnode;

                Stat stat = zooKeeper.exists(workerFullPath, false);

                if (stat == null) {
                    continue;
                }

                byte[] data = zooKeeper.getData(workerFullPath, false, stat);
                String address = new String(data);
                addresses.add(address);
            }

            this.allServiceAddresses = Collections.unmodifiableList(addresses);

            System.out.println("The cluster addresses are : " + allServiceAddresses);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddresses();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
