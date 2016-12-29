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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IQueue;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * The Hazelcast Output step stores value objects, for the given key names, to Hazelcast server(s).
 */
public class HazelcastOutput extends BaseStep implements StepInterface {
    private static Class<?> PKG = HazelcastOutputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

    protected HazelcastOutputMeta meta;
    protected HazelcastOutputData data;

    protected HazelcastInstance client = null;

    public HazelcastOutput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                           Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (super.init(smi, sdi)) {
            try {
                // Create client and connect to Hazelcast server(s)
                HazelcastOutputMeta outputMeta = (HazelcastOutputMeta) smi;
                Set<InetSocketAddress> servers = outputMeta.getServers();
                ClientConfig clientConfig = new ClientConfig();
                ClientNetworkConfig clientNetConfig = new ClientNetworkConfig();
                if (servers != null) {

                    for (InetSocketAddress server : servers) {
                        clientNetConfig.addAddress(server.getHostName() + ":" + server.getPort());
                    }
                    clientConfig.setNetworkConfig(clientNetConfig);

                    // if there's a group config, apply it
                    if (!Const.isEmpty(outputMeta.getGroupName())) {
                        GroupConfig grpCfg = new GroupConfig(outputMeta.getGroupName(), outputMeta.getGroupPassword());
                        clientConfig.setGroupConfig(grpCfg);
                    }

                    client = HazelcastClient.newHazelcastClient(clientConfig);
                }
                return true;
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "HazelcastOutput.Error.ConnectError"), e);
                return false;
            }
        } else {
            return false;
        }
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (HazelcastOutputMeta) smi;
        data = (HazelcastOutputData) sdi;

        Object[] r = getRow(); // get row, set busy!

        // If no more input to be expected, stop
        if (r == null) {
            setOutputDone();
            return false;
        }

        if (first) {
            first = false;

            // clone input row meta for now, we will change it (add or set inline) later
            data.outputRowMeta = getInputRowMeta().clone();
            // Get output field types
            meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
        }

        Object[] outputRowData = r;

        // Get structure from Hazelcast
        String structName = fieldSubstitute(environmentSubstitute(meta.getStructureName()), getInputRowMeta(), r);
        String structType = meta.getStructureType();
        // TODO better way to pick which API method to call based on structure type
        if (structType == "Queue") {
            IQueue<SerializableRow> q = client.getQueue(structName);
            SerializableRow sRow = new SerializableRow(data.outputRowMeta, outputRowData);
            if (!q.offer(sRow)) {
                putError(data.outputRowMeta, outputRowData, 1,
                        BaseMessages.getString(PKG, "HazelcastOutput.Error.Row.PutRowInQ"), null, null);
            }
        }
        putRow(data.outputRowMeta, outputRowData); // copy row to possible alternate rowset(s).

        if (checkFeedback(getLinesRead())) {
            if (log.isBasic()) {
                logBasic(BaseMessages.getString(PKG, "HazelcastOutput.Log.LineNumber") + getLinesRead());
            }
        }

        return true;
    }
}
