// RubyBSONDecoder.java

package org.jbson;

import static org.bson.BSON.*;

import java.io.*;

import org.jruby.*;

import org.bson.*;
import org.bson.io.*;
import org.bson.types.*;

public class RubyBSONDecoder extends BSONDecoder {

//    public int decode( RubyString s , BSONCallback callback ){
//        byte[] b = s.getBytes();
//        try {
//            return decode( new Input( new ByteArrayInputStream(b) ) , callback );
//        }
//        catch ( IOException ioe ){
//            throw new RuntimeException( "should be impossible" , ioe );
//        }
//    }

}
