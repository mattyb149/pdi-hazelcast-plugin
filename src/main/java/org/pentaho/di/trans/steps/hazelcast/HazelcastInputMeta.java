/*******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2012 by Pentaho : http://www.pentaho.com
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

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
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
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

/**
 * The Hazelcast Input step looks up value objects, from the given key names, from Hazelcast server(s).
 * 
 */
@Step( id = "HazelcastInput", image = "hazelcast-input.png", name = "Hazelcast Input",
    description = "Reads from a memcached instance", categoryDescription = "Input" )
public class HazelcastInputMeta extends BaseStepMeta implements StepMetaInterface {
  private static Class<?> PKG = HazelcastInputMeta.class; // for i18n purposes, needed by Translator2!! $NON-NLS-1$

  private String mapFieldName;
  private String keyFieldName;
  private String keyTypeName;
  private String valueFieldName;
  private String valueTypeName;
  private Set<InetSocketAddress> servers;

  public HazelcastInputMeta() {
    super(); // allocate BaseStepMeta
  }

  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    readData( stepnode );
  }

  public Object clone() {
    HazelcastInputMeta retval = (HazelcastInputMeta) super.clone();
    retval.setKeyFieldName( this.keyFieldName );
    retval.setValueFieldName( this.valueFieldName );
    retval.setValueTypeName( this.valueTypeName );
    return retval;
  }

  public void allocate( int nrfields ) {
    servers = new HashSet<InetSocketAddress>();
  }

  public void setDefault() {
    this.keyFieldName = null;
    this.valueFieldName = null;
    this.valueTypeName = null;
  }

  public void getFields( RowMetaInterface inputRowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repository, IMetaStore metaStore ) throws KettleStepException {
    if ( !Const.isEmpty( this.valueFieldName ) ) {

      // Add value field meta if not found, else set it
      ValueMetaInterface v;
      try {
        v = ValueMetaFactory.createValueMeta( this.valueFieldName, ValueMeta.getType( this.valueTypeName ) );
      } catch ( KettlePluginException e ) {
        throw new KettleStepException( BaseMessages.getString( PKG,
            "HazelcastInputMeta.Exception.ValueTypeNameNotFound" ), e );
      }
      v.setOrigin( origin );
      int valueFieldIndex = inputRowMeta.indexOfValue( this.valueFieldName );
      if ( valueFieldIndex < 0 ) {
        inputRowMeta.addValueMeta( v );
      } else {
        inputRowMeta.setValueMeta( valueFieldIndex, v );
      }
    } else {
      throw new KettleStepException( BaseMessages
          .getString( PKG, "HazelcastInputMeta.Exception.ValueFieldNameNotFound" ) );
    }
  }

  public void check( List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
      String input[], String output[], RowMetaInterface info, VariableSpace space, Repository repository,
      IMetaStore metaStore ) {
    CheckResult cr;
    if ( prev == null || prev.size() == 0 ) {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_WARNING, BaseMessages.getString( PKG,
              "HazelcastInputMeta.CheckResult.NotReceivingFields" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "HazelcastInputMeta.CheckResult.StepRecevingData", prev.size() + "" ), stepMeta );
      remarks.add( cr );
    }

    // See if we have input streams leading to this step!
    if ( input.length > 0 ) {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_OK, BaseMessages.getString( PKG,
              "HazelcastInputMeta.CheckResult.StepRecevingData2" ), stepMeta );
      remarks.add( cr );
    } else {
      cr =
          new CheckResult( CheckResultInterface.TYPE_RESULT_ERROR, BaseMessages.getString( PKG,
              "HazelcastInputMeta.CheckResult.NoInputReceivedFromOtherSteps" ), stepMeta );
      remarks.add( cr );
    }
  }

  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
      Trans trans ) {
    return new HazelcastInput( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  public StepDataInterface getStepData() {
    return new HazelcastInputData();
  }
  
  public String getMapFieldName() {
    return mapFieldName;
  }

  public void setMapFieldName( String mapFieldName ) {
    this.mapFieldName = mapFieldName;
  }

  public String getKeyFieldName() {
    return keyFieldName;
  }

  public void setKeyFieldName( String keyFieldName ) {
    this.keyFieldName = keyFieldName;
  }
  
  public String getKeyTypeName() {
    return keyTypeName;
  }

  public void setKeyTypeName( String keyTypeName ) {
    this.keyTypeName = keyTypeName;
  }

  public String getValueFieldName() {
    return valueFieldName;
  }

  public void setValueFieldName( String valueFieldName ) {
    this.valueFieldName = valueFieldName;
  }

  public String getValueTypeName() {
    return valueTypeName;
  }

  public void setValueTypeName( String mapFieldName ) {
    this.valueTypeName = mapFieldName;
  }
  
  public Set<InetSocketAddress> getServers() {
    return servers;
  }

  public void setServers( Set<InetSocketAddress> servers ) {
    this.servers = servers;
  }  

  @Override
  public String getXML() throws KettleException {
    StringBuffer retval = new StringBuffer();
    retval.append( "    " + XMLHandler.addTagValue( "mapfield", this.getMapFieldName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "keyfield", this.getKeyFieldName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "keytype", this.getKeyTypeName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "valuefield", this.getValueFieldName() ) );
    retval.append( "    " + XMLHandler.addTagValue( "valuetype", this.getValueTypeName() ) );
    retval.append( "    <servers>" ).append( Const.CR );
    Set<InetSocketAddress> servers = this.getServers();
    if ( servers != null ) {
      for ( InetSocketAddress addr : servers ) {
        retval.append( "      <server>" ).append( Const.CR );
        retval.append( "        " ).append( XMLHandler.addTagValue( "hostname", addr.getHostName() ) );
        retval.append( "        " ).append( XMLHandler.addTagValue( "port", addr.getPort() ) );
        retval.append( "      </server>" ).append( Const.CR );
      }
    }
    retval.append( "    </servers>" ).append( Const.CR );

    return retval.toString();
  }

  private void readData( Node stepnode ) throws KettleXMLException {
    try {
      this.mapFieldName = XMLHandler.getTagValue( stepnode, "mapfield" );
      this.keyFieldName = XMLHandler.getTagValue( stepnode, "keyfield" );
      this.keyTypeName = XMLHandler.getTagValue( stepnode, "keytype" );
      this.valueFieldName = XMLHandler.getTagValue( stepnode, "valuefield" );
      this.valueTypeName = XMLHandler.getTagValue( stepnode, "valuetype" );
      Node serverNodes = XMLHandler.getSubNode( stepnode, "servers" );
      int nrservers = XMLHandler.countNodes( serverNodes, "server" );

      allocate( nrservers );

      for ( int i = 0; i < nrservers; i++ ) {
        Node fnode = XMLHandler.getSubNodeByNr( serverNodes, "server", i );

        String hostname = XMLHandler.getTagValue( fnode, "hostname" );
        int port = Integer.parseInt( XMLHandler.getTagValue( fnode, "port" ) );
        servers.add( new InetSocketAddress( hostname, port ) );
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG, "HazelcastInputMeta.Exception.UnableToReadStepInfo" ),
          e );
    }
  }

  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    try {
      this.mapFieldName = rep.getStepAttributeString( id_step, "mapfield" );
      this.keyFieldName = rep.getStepAttributeString( id_step, "keyfield" );
      this.keyTypeName = rep.getStepAttributeString( id_step, "keytype" );
      this.valueFieldName = rep.getStepAttributeString( id_step, "valuefield" );
      this.valueTypeName = rep.getStepAttributeString( id_step, "valuetype" );

      int nrservers = rep.countNrStepAttributes( id_step, "server" );

      allocate( nrservers );

      for ( int i = 0; i < nrservers; i++ ) {
        servers.add( new InetSocketAddress( rep.getStepAttributeString( id_step, i, "hostname" ), Integer.parseInt( rep
            .getStepAttributeString( id_step, i, "port" ) ) ) );
      }

    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "HazelcastInputMeta.Exception.UnexpectedErrorReadingStepInfo" ), e );
    }
  }

  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    try {
      rep.saveStepAttribute( id_transformation, id_step, "mapfield", this.mapFieldName );
      rep.saveStepAttribute( id_transformation, id_step, "keyfield", this.keyFieldName );
      rep.saveStepAttribute( id_transformation, id_step, "keytype", this.keyTypeName );
      rep.saveStepAttribute( id_transformation, id_step, "valuefield", this.valueFieldName );
      rep.saveStepAttribute( id_transformation, id_step, "valuetype", this.valueTypeName );
      int i = 0;
      Set<InetSocketAddress> servers = this.getServers();
      if ( servers != null ) {
        for ( InetSocketAddress addr : servers ) {
          rep.saveStepAttribute( id_transformation, id_step, i++, "hostname", addr.getHostName() );
          rep.saveStepAttribute( id_transformation, id_step, i++, "port", addr.getPort() );
        }
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
          "HazelcastInputMeta.Exception.UnexpectedErrorSavingStepInfo" ), e );
    }
  }
}
