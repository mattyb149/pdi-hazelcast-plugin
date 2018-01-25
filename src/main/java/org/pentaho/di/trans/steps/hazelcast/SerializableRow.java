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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by janosveres on 2/08/17.
 */
public class SerializableRow implements Serializable {

    private String rowDataXML;
    private String rowMetaXML;
    private Object[] data;
    private transient RowMeta rm;

    public SerializableRow() {
        this.clear();
    }

    public SerializableRow(String rowMetaXML, String rowDataXML, Object... data) {
        this.rowMetaXML = rowMetaXML;
        this.rowDataXML = rowMetaXML;
        // in any case
        this.data = data;
    }

    public SerializableRow(RowMetaInterface rowMeta, Object... data) {
        this.data = data;
        try {
            this.rowDataXML = rowMeta.getDataXML(data);
            this.rowMetaXML = rowMeta.getMetaXML();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws ParserConfigurationException, KettleException, IOException, SAXException {
        String xml = "<row-meta>" +
                "<value-meta>" +
                "<type>String</type>" +
                "<storagetype>normal</storagetype>" +
                "<name>test1</name>" +
                "<length>-1</length>" +
                "<precision>-1</precision>" +
                "<origin>Generate Rows</origin>" +
                "<comments/>" +
                "<conversion_Mask/>" +
                "<decimal_symbol/>" +
                "<grouping_symbol/>" +
                "<currency_symbol/>" +
                "<trim_type>none</trim_type>" +
                "<case_insensitive>N</case_insensitive>" +
                "<collator_disabled>Y</collator_disabled>" +
                "<collator_strength>0</collator_strength>" +
                "<sort_descending>N</sort_descending>" +
                "<output_padding>N</output_padding>" +
                "<date_format_lenient>N</date_format_lenient>" +
                "<date_format_locale>en_US</date_format_locale>" +
                "<date_format_timezone>Europe&#x2f;Berlin</date_format_timezone>" +
                "<lenient_string_to_number>N</lenient_string_to_number>" +
                "</value-meta>" +
                "<value-meta>" +
                "<type>String</type>" +
                "<storagetype>normal</storagetype>" +
                "<name>test2</name>" +
                "<length>-1</length>" +
                "<precision>-1</precision>" +
                "<origin>Generate Rows</origin>" +
                "<comments/>" +
                "<conversion_Mask/>" +
                "<decimal_symbol/>" +
                "<grouping_symbol/>" +
                "<currency_symbol/>" +
                "<trim_type>none</trim_type>" +
                "<case_insensitive>N</case_insensitive>" +
                "<collator_disabled>Y</collator_disabled>" +
                "<collator_strength>0</collator_strength>" +
                "<sort_descending>N</sort_descending>" +
                "<output_padding>N</output_padding>" +
                "<date_format_lenient>N</date_format_lenient>" +
                "<date_format_locale>en_US</date_format_locale>" +
                "<date_format_timezone>Europe&#x2f;Berlin</date_format_timezone>" +
                "<lenient_string_to_number>N</lenient_string_to_number>" +
                "</value-meta>" +
                "<value-meta>" +
                "<type>Date</type>" +
                "<storagetype>normal</storagetype>" +
                "<name>dateee</name>" +
                "<length>-1</length>" +
                "<precision>-1</precision>" +
                "<origin>Generate Rows</origin>" +
                "<comments/>" +
                "<conversion_Mask>dd-MM-yyyy</conversion_Mask>" +
                "<decimal_symbol/>" +
                "<grouping_symbol/>" +
                "<currency_symbol/>" +
                "<trim_type>none</trim_type>" +
                "<case_insensitive>N</case_insensitive>" +
                "<collator_disabled>Y</collator_disabled>" +
                "<collator_strength>0</collator_strength>" +
                "<sort_descending>N</sort_descending>" +
                "<output_padding>N</output_padding>" +
                "<date_format_lenient>N</date_format_lenient>" +
                "<date_format_locale>en_US</date_format_locale>" +
                "<date_format_timezone>Europe&#x2f;Berlin</date_format_timezone>" +
                "<lenient_string_to_number>N</lenient_string_to_number>" +
                "</value-meta>" +
                "<value-meta>" +
                "<type>Boolean</type>" +
                "<storagetype>normal</storagetype>" +
                "<name>bollcsi</name>" +
                "<length>-1</length>" +
                "<precision>-1</precision>" +
                "<origin>Generate Rows</origin>" +
                "<comments/>" +
                "<conversion_Mask/>" +
                "<decimal_symbol/>" +
                "<grouping_symbol/>" +
                "<currency_symbol/>" +
                "<trim_type>none</trim_type>" +
                "<case_insensitive>N</case_insensitive>" +
                "<collator_disabled>Y</collator_disabled>" +
                "<collator_strength>0</collator_strength>" +
                "<sort_descending>N</sort_descending>" +
                "<output_padding>N</output_padding>" +
                "<date_format_lenient>N</date_format_lenient>" +
                "<date_format_locale>en_US</date_format_locale>" +
                "<date_format_timezone>Europe&#x2f;Berlin</date_format_timezone>" +
                "<lenient_string_to_number>N</lenient_string_to_number>" +
                "</value-meta>" +
                "</row-meta>";

        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new org.xml.sax.InputSource(new StringReader(xml)));
        NodeList nodeList = doc.getElementsByTagName("value-meta");

        RowMeta rm = new RowMeta(); // load meta
        List<String> typeCodes = new ArrayList<String>();
        for (String typeCode : ValueMetaInterface.typeCodes) {
            typeCodes.add(typeCode);
        }
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            NodeList childNodes = node.getChildNodes();
            String name = childNodes.item(2).getTextContent();
            String typeText = childNodes.item(0).getTextContent();

            int type = typeCodes.indexOf(typeText);
            rm.addValueMeta(new ValueMeta(name, type));
        }

        System.out.println("Type of first column: " + rm.getValueMeta(0).getTypeDesc());
    }

    public void clear() {
        this.rowDataXML = "";
        this.rowMetaXML = "";
        this.data = new Object[0];
    }

    public void prepareExtraction() {
        try {
            // the metadata
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new org.xml.sax.InputSource(new StringReader(rowMetaXML)));
            rm = new RowMeta(doc.getFirstChild()); // load meta
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

    public Object[] getData() {
        return this.data;
    }

    public void setData(Object[] data) {
        this.data = data;
    }

    public String getRowDataXML() {
        return this.rowDataXML;
    }

    public void setRowDataXML(String rowDataXML) {
        this.rowDataXML = rowDataXML;
    }

    public String getRowMetaXML() {
        return rowMetaXML;
    }

    public void setRowMetaXML(String rowMetaXML) {
        this.rowMetaXML = rowMetaXML;
        if (rowMetaXML != null)
            prepareExtraction();
    }

    public String getString(int index) throws KettleValueException {
        return rm.getString(data, index);
    }

    public Long getInteger(int index) throws KettleValueException {
        return rm.getInteger(data, index);
    }

    public Double getNumber(int index) throws KettleValueException {
        return rm.getNumber(data, index);
    }

    public Date getDate(int index) throws KettleValueException {
        return rm.getDate(data, index);
    }

    public BigDecimal getBigNumber(int index) throws KettleValueException {
        return rm.getBigNumber(data, index);
    }

    public Boolean getBoolean(int index) throws KettleValueException {
        return rm.getBoolean(data, index);
    }

    public byte[] getBinary(int index) throws KettleValueException {
        return rm.getBinary(data, index);
    }

    public Double getNumber(String valueName, Double defaultValue) throws KettleValueException {
        Double number = getNumber(rm.indexOfValue(valueName));
        return number == null ? defaultValue : number;
    }

    public BigDecimal getBigNumber(String valueName, BigDecimal defaultValue) throws KettleValueException {
        BigDecimal bigNumber = getBigNumber(rm.indexOfValue(valueName));
        return bigNumber == null ? defaultValue : bigNumber;
    }

    public Boolean getBoolean(String valueName, Boolean defaultValue) throws KettleValueException {
        Boolean boolv = getBoolean(rm.indexOfValue(valueName));
        return boolv == null ? defaultValue : boolv;
    }

    public byte[] getBinary(String valueName, byte[] defaultValue) throws KettleValueException {
        byte[] bin = getBinary(rm.indexOfValue(valueName));
        return bin == null ? defaultValue : bin;
    }

    public String getString(String valueName, String defaultValue) throws KettleValueException {
        return rm.getString(data, valueName, defaultValue);
    }

    public Long getInteger(String valueName, Long defaultValue) throws KettleValueException {
        return rm.getInteger(data, valueName, defaultValue);
    }

    public Date getDate(String valueName, Date defaultValue) throws KettleValueException {
        return rm.getDate(data, valueName, defaultValue);
    }
}