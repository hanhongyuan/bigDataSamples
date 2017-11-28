package com.unimas.server.node;

import com.unimas.beans.NodeInfo;
import com.unimas.common.JsonUtils;
import com.unimas.Context;
import com.unimas.zk.ZkClient;
import com.unimas.zk.ZkConfig;
import com.unimas.zk.ZkUtils;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;


/**
 */
public class RegisterNode {

    private final Logger logger = Logger.getLogger(RegisterNode.class);
    private final String path;
    private final ZkClient zkClient;

    public RegisterNode(ZkClient zkClient) {
        this.zkClient = zkClient;
        this.path = ZkConfig.zk_nodes + "/" + Context.getInstance().getLocalNodeId();
    }

    public void addLostListener() {
        zkClient.getCurator().getConnectionStateListenable().addListener((client, newState) -> {

            if (newState == ConnectionState.LOST) {
                while (true) {
                    try {
                        logger.info("try to register node: " + Context.getInstance().getLocalNodeId());
                        if (zkClient.getCurator().getZookeeperClient().blockUntilConnectedOrTimedOut()) {
                            register();
                            break;
                        }
                    } catch (Exception e) {
                        logger.error(e);
                    }
                }
            }
        });
    }

    public void register() throws Exception {
        Context context = Context.getInstance();
        NodeInfo nodeInfo = new NodeInfo(context.getLocalNodeId(), context.getLocalHost(), context.getLocalPort());
        String content = JsonUtils.toJson(nodeInfo);
        ZkUtils.create(zkClient, path, content, CreateMode.EPHEMERAL);
    }

}

