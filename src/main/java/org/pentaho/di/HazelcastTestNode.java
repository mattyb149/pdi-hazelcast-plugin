package org.pentaho.di;

import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import org.pentaho.di.trans.steps.hazelcast.SerializableRow;

import java.io.IOException;

/**
 * Created by janosveres on 07/02/2017.
 */
public class HazelcastTestNode implements ItemListener<SerializableRow> {

    private final HazelcastInstance hazelcastInstance;

    private HazelcastTestNode() {
        Config cfg = new Config();
        cfg.setInstanceName("test-node");
        cfg.getGroupConfig().setName("test-group");
        cfg.getGroupConfig().setPassword("01234");

        hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
    }

    public static void main(String[] args) {
        HazelcastTestNode node = new HazelcastTestNode();

        IQueue<SerializableRow> quque = node.hazelcastInstance.getQueue("queue");
        quque.addItemListener(node, true);

        System.out.println("Press enter to exit");
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (node.hazelcastInstance != null)
            node.hazelcastInstance.shutdown();
    }

    @Override
    public void itemAdded(ItemEvent<SerializableRow> item) {
        System.out.println("object added: " + item.toString());
    }

    @Override
    public void itemRemoved(ItemEvent<SerializableRow> item) {
        System.out.println("object removed: " + item.toString());
    }
}
