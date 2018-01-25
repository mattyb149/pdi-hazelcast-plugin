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
import com.hazelcast.core.IMap;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * The Hazelcast Input step looks up value objects, from the given key names, from memached server(s).
 */
public class HazelcastInput extends BaseStep implements StepInterface {
    private static Class<?> PKG = HazelcastInputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

    protected HazelcastInputMeta meta;
    protected HazelcastInputData data;

    protected HazelcastInstance client = null;

    public HazelcastInput(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
                          Trans trans) {
        super(stepMeta, stepDataInterface, copyNr, transMeta, trans);
    }

    @Override
    public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
        if (super.init(smi, sdi)) {
            try {
                // Create client and connect to Hazelcast server(s)
                HazelcastInputMeta inputMeta = (HazelcastInputMeta) smi;
                Set<InetSocketAddress> servers = inputMeta.getServers();
                ClientConfig clientConfig = new ClientConfig();
                ClientNetworkConfig clientNetConfig = new ClientNetworkConfig();
                if (servers != null) {

                    for (InetSocketAddress server : servers) {
                        clientNetConfig.addAddress(server.getHostName() + ":" + server.getPort());
                    }
                    clientConfig.setNetworkConfig(clientNetConfig);

                    // if there's a group config, apply it
                    if (!Const.isEmpty(inputMeta.getGroupName())) {
                        GroupConfig grpCfg = new GroupConfig(inputMeta.getGroupName(), inputMeta.getGroupPassword());
                        clientConfig.setGroupConfig(grpCfg);
                    }

                    // actually create client
                    client = HazelcastClient.newHazelcastClient(clientConfig);
                }
                return true;
            } catch (Exception e) {
                logError(BaseMessages.getString(PKG, "HazelcastInput.Error.ConnectError"), e);
                return false;
            }
        } else {
            return false;
        }
    }

    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
        meta = (HazelcastInputMeta) smi;
        data = (HazelcastInputData) sdi;

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

        // Get map from Hazelcast, don't cast now, be lazy. TODO change this?
        String mapName = fieldSubstitute(environmentSubstitute(meta.getStructureName()), getInputRowMeta(), r);
        int mapFieldIndex = getInputRowMeta().indexOfValue(mapName);
        if (mapFieldIndex < 0) {
            logBasic(BaseMessages.getString(PKG, "HazelcastInput.Warning.NotFound.StructureName", mapName));
        }

        // Get key name
        int keyFieldIndex = getInputRowMeta().indexOfValue(meta.getKeyFieldName());
        if (keyFieldIndex < 0) {
            throw new KettleException(BaseMessages.getString(PKG, "HazelcastInputMeta.Exception.KeyFieldNameNotFound"));
        }

        // Fetch value from Hazelcast, don't cast now, be lazy. TODO change this?
        IMap<?, ?> inputMap = client.getMap(mapName);
        Object fetchedValue = inputMap.get(r[keyFieldIndex]);

        // Add Value data name to output, or set value data if already exists
        Object[] outputRowData = r;
        int valueFieldIndex = getInputRowMeta().indexOfValue(environmentSubstitute(meta.getValueFieldName()));
        if (valueFieldIndex < 0 || valueFieldIndex > outputRowData.length) {
            // Not found so add it
            outputRowData = RowDataUtil.addValueData(r, getInputRowMeta().size(), fetchedValue);
        } else {
            // Update value in place
            outputRowData[valueFieldIndex] = fetchedValue;
        }

        putRow(data.outputRowMeta, outputRowData); // copy row to possible alternate rowset(s).

        if (checkFeedback(getLinesRead())) {
            if (log.isBasic())
                logBasic(BaseMessages.getString(PKG, "HazelcastInput.Log.LineNumber") + getLinesRead());
        }

        return true;
    }
}
