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


  }

}
