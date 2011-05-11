// RubyBSONDecoder.java

package org.jbson;

import static org.bson.BSON.*;

import java.io.*;

import org.jruby.*;

import org.bson.*;
import org.bson.io.*;
import org.bson.types.*;

public class RubyBSONDecoder extends BSONDecoder {

      protected void _binary( String name )
          throws IOException {
          final int totalLen = _in.readInt();
          final byte bType = _in.read();

          if( bType == 2 ) {
              final int len = _in.readInt();
              if ( len + 4 != totalLen )
                  throw new IllegalArgumentException( "bad data size subtype 2 len: " + len + "totalLen: " + totalLen );

              final byte[] data = new byte[len];
              _in.fill( data );
              _callback.gotBinary( name, (byte)2, data );
          }
          else {
              byte[] data = new byte[totalLen];
              _in.fill( data );
              _callback.gotBinary( name, bType, data );
          }
     }
}
