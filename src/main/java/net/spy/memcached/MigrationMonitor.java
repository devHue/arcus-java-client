package net.spy.memcached;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.internal.MigrationMode;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;

import java.util.List;

public class MigrationMonitor extends SpyObject {


    private final static String joiningListPath = "joining_list";

    private final static String leavingListPath = "leaving_list";

    private ZooKeeper zk;

    private String cloudStatZpath;

    private String serviceCode;

    volatile boolean dead;

    private MigrationMonitorListener listener;

    private MigrationWatcher migrationWatcher;

    private MigrationWatcher alterListWatcher;

    private MigrationMode mode;

    /** Watcher for the service
     *
     */

    public MigrationMonitor(ZooKeeper zk, String cloudStatZpath, String serviceCode,
                            final MigrationMonitorListener listener) {
        this.zk = zk;
        this.cloudStatZpath = cloudStatZpath;
        this.serviceCode = serviceCode;
        this.listener = listener;

        migrationWatcher = new MigrationWatcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    asyncGetMigrationList();
                }
            }

            @Override
            public void processResult(int i, String s, Object o, List<String> list) {
                Code code = Code.get(i);
                switch (code) {
                case OK :
                    listener.commandMigrationNodeChange(list);
                    break;
                case SESSIONEXPIRED:
                    getLogger().warn("Session expired. Trying to reconnect to the Arcus admin. " + getInfo());
                    shutdown();
                    break;
                case NOAUTH:
                    getLogger().fatal("Authorization failed " + getInfo());
                    shutdown();
                    break;
                case NONODE:
                    getLogger().fatal("Cannot find your service code. Please contact Arcus support to solve this problem. " + getInfo());
                    break;
                case CONNECTIONLOSS:
                    getLogger().warn("Connection lost. Trying to reconnect to the Arcus admin." + getInfo());
                    asyncGetMigrationList();
                    break;
                default:
                    getLogger().warn("Ignoring an unexpected event from the Arcus admin. code=" + code + ", " + getInfo());
                    asyncGetMigrationList();
                    break;
                }
            }
        };

        alterListWatcher = new MigrationWatcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                    asyncGetMigrationStatus();
                }
            }

            @Override
            public void processResult(int i, String s, Object o, List<String> list) {
                Code code = Code.get(i);
                switch (code) {
                    case OK:
                        //set a new Watcher for joining list or leaving list
                        setMigrationMode(list);
                        return;
                    case SESSIONEXPIRED:
                        getLogger().warn("Session expired. Trying to reconnect to the Arcus admin. " + getInfo());
                        shutdown();
                        return;
                    case NOAUTH:
                        getLogger().fatal("Authorization failed " + getInfo());
                        shutdown();
                        return;
                    case NONODE:
                        getLogger().fatal("Cannot find your service code. Please contact Arcus support to solve this problem. " + getInfo());
                        initMigration();
                        return;
                    case CONNECTIONLOSS:
                        getLogger().warn("Connection lost. Trying to reconnect to the Arcus admin." + getInfo());
                        asyncGetMigrationStatus();
                        return;
                    default:
                        getLogger().warn("Ignoring an unexpected event from the Arcus admin. code=" + code + ", " + getInfo());
                        asyncGetMigrationStatus();
                        return;
                }
            }
        };
        initMigration();
    }

    public void initMigration() {
        getLogger().info("Initializing the MigrationMonitor");
        mode = MigrationMode.Init;
        asyncGetMigrationStatus();
    }

    private void asyncGetMigrationStatus() {
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("Set a new watch on " + (cloudStatZpath + serviceCode) + " for Migration");
        }
        zk.getChildren(cloudStatZpath + serviceCode, alterListWatcher, alterListWatcher, null);
    }

    private void setMigrationMode(List<String> children) {
        if(children.contains(joiningListPath)) {
            mode = MigrationMode.Join;
            asyncGetMigrationList();
        } else if (children.contains(leavingListPath)) {
            mode = MigrationMode.Leave;
            asyncGetMigrationList();
        }
    }

    private void asyncGetMigrationList() {
        switch (mode) {
        case Join:
            if(migrationWatcher != null) {
                zk.getChildren(cloudStatZpath + serviceCode + "/" + joiningListPath, migrationWatcher, migrationWatcher, null);
            }
            break;
        case Leave:
            if(migrationWatcher != null) {
                zk.getChildren(cloudStatZpath + serviceCode + "/" + leavingListPath, migrationWatcher, migrationWatcher, null);
            }
            break;
        case Init:
            break;
        default:
            getLogger().error("Unexpected Migration Mode : " + mode);
        }
    }

    public void shutdown() {
        getLogger().info("Shutting down the MigrationMonitor. " + getInfo());
        dead = true;
        if(listener != null) {
            listener.closing();
        }
    }

    private String getInfo() {
        String zkSessionId = null;
        if(zk != null) {
            zkSessionId = "0x" + Long.toHexString(zk.getSessionId());
        }

        return "[serviceCode=" + serviceCode + ", adminSessionId=" + zkSessionId + "]";
    }

    public MigrationMode getMode() {
        return mode;
    }

    public interface MigrationMonitorListener {

        void commandMigrationNodeChange(List<String> children);

        List<String> getPrevMigrationChildren();

        void closing();
    }

    private interface MigrationWatcher extends Watcher, AsyncCallback.ChildrenCallback {
        @Override
        void process(WatchedEvent watchedEvent);

        @Override
        void processResult(int i, String s, Object o, List<String> list);
    }
}
