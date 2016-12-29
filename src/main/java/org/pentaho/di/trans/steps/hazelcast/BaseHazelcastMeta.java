/*******************************************************************************
 *
 * Copyright (C) 2014-2015 by Matt Burgess
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.steps.hazelcast;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.w3c.dom.Node;

import java.net.InetSocketAddress;
import java.util.HashSet;
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

    protected void writeServersToXml(StringBuffer xml, Set<InetSocketAddress> servers) {
        if (servers != null) {
            for (InetSocketAddress addr : servers) {
                xml.append("      <server>").append(Const.CR);
                xml.append("        ").append(XMLHandler.addTagValue("hostname", addr.getHostName()));
                xml.append("        ").append(XMLHandler.addTagValue("port", addr.getPort()));
                xml.append("      </server>").append(Const.CR);
            }
        }
    }

    protected void readServersFromXml(Node serverNodes, int nrservers) {
        for (int i = 0; i < nrservers; i++) {
            Node fnode = XMLHandler.getSubNodeByNr(serverNodes, "server", i);

            String hostname = XMLHandler.getTagValue(fnode, "hostname");
            int port = Integer.parseInt(XMLHandler.getTagValue(fnode, "port"));
            servers.add(new InetSocketAddress(hostname, port));
        }
    }

    protected void readServerFromRep(Repository rep, ObjectId id_step, int nrservers) throws KettleException {
        for (int i = 0; i < nrservers; i++) {
            servers.add(new InetSocketAddress(rep.getStepAttributeString(id_step, i, "hostname"), Integer.parseInt(rep
                    .getStepAttributeString(id_step, i, "port"))));
        }
    }

    public void allocate(int nrfields) {
        servers = new HashSet<InetSocketAddress>(nrfields);
    }

    protected void writeServerToRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
        int i = 0;
        Set<InetSocketAddress> servers = this.getServers();
        if (servers != null) {
            for (InetSocketAddress addr : servers) {
                rep.saveStepAttribute(id_transformation, id_step, i++, "hostname", addr.getHostName());
                rep.saveStepAttribute(id_transformation, id_step, i++, "port", addr.getPort());
            }
        }
    }
}
