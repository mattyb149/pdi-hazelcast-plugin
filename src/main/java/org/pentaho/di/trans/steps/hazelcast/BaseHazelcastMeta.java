package org.pentaho.di.trans.steps.hazelcast;

import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Created by mburgess on 3/23/15.
 */
public abstract class BaseHazelcastMeta extends BaseStepMeta implements StepMetaInterface {

  protected Set<InetSocketAddress> servers;

  public Set<InetSocketAddress> getServers() {
    return servers;
  }

  public void setServers( Set<InetSocketAddress> servers ) {
    this.servers = servers;
  }
}
