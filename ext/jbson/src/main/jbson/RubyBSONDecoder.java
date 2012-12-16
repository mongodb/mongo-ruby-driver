package org.jbson;

import java.io.*;

import org.jruby.*;

import org.bson.*;
import org.bson.io.*;
import org.bson.types.*;
import static org.bson.BSON.*;

public class RubyBSONDecoder extends BasicBSONDecoder {

      protected void _binary( String name )
          throws IOException {
          final int totalLen = _in.readInt();
          final byte bType = _in.read();

          if( bType == 2 ) {
              final int len = _in.readInt();
              if ( len + 4 != totalLen )
                  throw new IllegalArgumentException("Error! Bad data size (Sub-Type 2 Length: " + len +
                                                     ", Total Length: " + totalLen );

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
