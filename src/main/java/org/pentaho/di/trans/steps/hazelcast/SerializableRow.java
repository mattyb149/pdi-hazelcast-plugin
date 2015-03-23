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

import org.pentaho.di.core.RowMetaAndData;
import org.pentaho.di.core.exception.KettleFileException;
import org.pentaho.di.core.row.RowMetaInterface;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;

/**
 * Created by mburgess on 3/19/15.
 */
public class SerializableRow implements Serializable {

  private RowMetaAndData row;

  public SerializableRow( RowMetaAndData row ) {
    this.row = row;
  }

  public SerializableRow( RowMetaInterface rowMeta, Object[] rowData) {
    row = new RowMetaAndData( rowMeta, rowData );
  }

  public RowMetaAndData getRow() {
    return row;
  }

  public void setRow( RowMetaAndData row ) {
    this.row = row;
  }

  private void writeObject( java.io.ObjectOutputStream out ) throws IOException {
    try {
      row.getRowMeta().writeData( new DataOutputStream( out ), row.getData() );
    } catch ( KettleFileException kfe ) {
      throw new IOException( kfe );
    }

  }

  private void readObject( java.io.ObjectInputStream in ) throws IOException, ClassNotFoundException {
    try {
      row.getRowMeta().readData( new DataInputStream( in ) );
    } catch ( KettleFileException kfe ) {
      throw new IOException( kfe );
    }
  }

  private void readObjectNoData() throws ObjectStreamException {
    // TODO?
  }

}
