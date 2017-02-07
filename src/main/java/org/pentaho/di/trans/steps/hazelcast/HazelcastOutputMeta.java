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

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The Hazelcast Output step writes value objects, for the given key names, to Hazelcast server(s).
 */
@Step(id = "HazelcastOutput", image = "hazelcast-output.png", name = "Hazelcast Output",
        description = "Writes to a Hazelcast instance", categoryDescription = "Output")
public class HazelcastOutputMeta extends BaseHazelcastMeta {
    public static final String XML_FIELDS_TAG = "fields";
    public static final String STRUCTNAME_TAG = "structname";
    private static Class<?> PKG = HazelcastOutputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$
    private String structureName;
    private EStructureType structureType;
    private String groupName;
    private String groupPassword;

    private List<ValueMetaInterface> fields;
    private int expirationTime;

    public HazelcastOutputMeta() {
        super(); // allocate BaseStepMeta
    }

    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
        readData(stepnode);
    }

    public Object clone() {
        HazelcastOutputMeta retval = (HazelcastOutputMeta) super.clone();
        retval.setStructureName(this.structureName);
        retval.setFields(this.fields);
        retval.setExpirationTime(this.expirationTime);
        retval.setGroupName(this.groupName);
        retval.setGroupPassword(this.groupPassword);
        retval.setStructureType(this.structureType);
        return retval;
    }

    public void setDefault() {
        this.structureName = null;
        this.fields = new ArrayList<ValueMetaInterface>();
        this.expirationTime = 0;
        this.groupName = null;
        this.groupPassword = null;
        this.structureType = EStructureType.Undefined;
    }

    public void getFields(RowMetaInterface inputRowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

        super.getFields(inputRowMeta, origin, info, nextStep, space, repository, metaStore);
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                      String input[], String output[], RowMetaInterface info, VariableSpace space, Repository repository,
                      IMetaStore metaStore) {
        CheckResult cr;
        if (prev == null || prev.size() == 0) {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString(PKG,
                            "HazelcastOutputMeta.CheckResult.NotReceivingFields"), stepMeta);
            remarks.add(cr);
        } else {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                            "HazelcastOutputMeta.CheckResult.StepRecevingData", prev.size() + ""), stepMeta);
            remarks.add(cr);
        }

        // See if we have input streams leading to this step!
        if (input.length > 0) {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString(PKG,
                            "HazelcastOutputMeta.CheckResult.StepRecevingData2"), stepMeta);
            remarks.add(cr);
        } else {
            cr =
                    new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString(PKG,
                            "HazelcastOutputMeta.CheckResult.NoInputReceivedFromOtherSteps"), stepMeta);
            remarks.add(cr);
        }
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                 Trans trans) {
        return new HazelcastOutput(stepMeta, stepDataInterface, cnr, tr, trans);
    }

    public StepDataInterface getStepData() {
        return new HazelcastOutputData();
    }

    public String getStructureName() {
        return structureName;
    }

    public void setStructureName(String structureName) {
        this.structureName = structureName;
    }

    public EStructureType getStructureType() {
        return structureType;
    }

    public void setStructureType(EStructureType structureType) {
        this.structureType = structureType;
    }

    public int getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(int expirationTime) {
        this.expirationTime = expirationTime;
    }

    public List<ValueMetaInterface> getFields() {
        return fields;
    }

    public void setFields(List<ValueMetaInterface> fields) {
        this.fields = fields;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getGroupPassword() {
        return groupPassword;
    }

    public void setGroupPassword(String groupPassword) {
        this.groupPassword = groupPassword;
    }

    @Override
    public String getXML() throws KettleException {
        try {
            StringBuffer xml = new StringBuffer();
            xml.append("    ").append(XMLHandler.addTagValue(STRUCTNAME_TAG, this.getStructureName()));
            xml.append("    " + XMLHandler.addTagValue("structuretype", this.getStructureType().toString()));
            xml.append("    ").append(XMLHandler.openTag(XML_FIELDS_TAG));
            for (ValueMetaInterface valueMeta : fields) {
                String valueMetaXml = valueMeta.getMetaXML();
                xml.append("        ").append(valueMetaXml);
            }
            xml.append("    ").append(XMLHandler.closeTag(XML_FIELDS_TAG));

            xml.append("    ").append(XMLHandler.addTagValue("expiration", this.getExpirationTime()));
            xml.append("    " + XMLHandler.addTagValue("groupname", this.getGroupName()));
            xml.append("    " + XMLHandler.addTagValue("grouppassword", this.getGroupPassword()));

            xml.append("    <servers>").append(Const.CR);
            Set<InetSocketAddress> servers = this.getServers();
            writeServersToXml(xml, servers);
            xml.append("    </servers>").append(Const.CR);

            return xml.toString();
        } catch (IOException ioe) {
            throw new KettleException(ioe);
        }
    }

    private void readData(Node stepnode) throws KettleXMLException {
        try {
            this.structureName = XMLHandler.getTagValue(stepnode, STRUCTNAME_TAG);
            this.structureType = EStructureType.valueOf(XMLHandler.getTagValue(stepnode, "structuretype"));
            // get the metadata
            Node fieldsNode = XMLHandler.getSubNode(stepnode, XML_FIELDS_TAG);
            RowMeta rowMeta = new RowMeta(XMLHandler.getSubNode(fieldsNode, RowMeta.XML_META_TAG));
            fields = rowMeta.getValueMetaList();

            this.expirationTime = Integer.parseInt(XMLHandler.getTagValue(stepnode, "expiration"));
            this.groupName = XMLHandler.getTagValue(stepnode, "groupname");
            this.groupPassword = XMLHandler.getTagValue(stepnode, "grouppassword");

            Node serverNodes = XMLHandler.getSubNode(stepnode, "servers");
            int nrservers = XMLHandler.countNodes(serverNodes, "server");

            allocate(nrservers);
            readServersFromXml(serverNodes, nrservers);
        } catch (Exception e) {
            throw new KettleXMLException(
                    BaseMessages.getString(PKG, "HazelcastOutputMeta.Exception.UnableToReadStepInfo"), e);
        }
    }

    public void readRep(Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases)
            throws KettleException {
        try {
            this.structureName = rep.getStepAttributeString(id_step, STRUCTNAME_TAG);
            String strtype = rep.getStepAttributeString(id_step, "structuretype");

            if (!Const.isEmpty(strtype))
                this.structureType = Enum.valueOf(EStructureType.class, strtype);
            int nrFields = rep.countNrStepAttributes(id_step, "field_name");
            fields.clear();
            for (int i = 0; i < nrFields; i++) {
                String fieldName = rep.getStepAttributeString(id_step, i, "field_name");
                int fieldType = ValueMeta.getType(rep.getStepAttributeString(id_step, i, "field_type"));

                ValueMetaInterface field = ValueMetaFactory.createValueMeta(fieldName, fieldType);
                fields.add(field);
            }
            this.expirationTime = (int) rep.getStepAttributeInteger(id_step, "expiration");
            this.groupName = rep.getStepAttributeString(id_step, "groupname");
            this.groupPassword = rep.getStepAttributeString(id_step, "grouppassword");

            int nrservers = rep.countNrStepAttributes(id_step, "server");

            allocate(nrservers);

            readServerFromRep(rep, id_step, nrservers);
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG,
                    "HazelcastOutputMeta.Exception.UnexpectedErrorReadingStepInfo"), e);
        }
    }

    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step)
            throws KettleException {
        try {
            rep.saveStepAttribute(id_transformation, id_step, STRUCTNAME_TAG, this.structureName);
            if (this.structureType != EStructureType.Undefined)
                rep.saveStepAttribute(id_transformation, id_step, "structuretype", this.structureType.toString());

            for (int i = 0; i < fields.size(); i++) {

                ValueMetaInterface field = fields.get(i);
                rep.saveStepAttribute(id_transformation, id_step, i, "field_name", field.getName());
                rep.saveStepAttribute(id_transformation, id_step, i, "field_type", field.getTypeDesc());
            }

            rep.saveStepAttribute(id_transformation, id_step, "expiration", this.expirationTime);
            rep.saveStepAttribute(id_transformation, id_step, "groupname", this.groupName);
            rep.saveStepAttribute(id_transformation, id_step, "grouppassword", this.groupPassword);
            writeServerToRep(rep, id_transformation, id_step);
        } catch (Exception e) {
            throw new KettleException(BaseMessages.getString(PKG,
                    "HazelcastOutputMeta.Exception.UnexpectedErrorSavingStepInfo"), e);
        }
    }

}
