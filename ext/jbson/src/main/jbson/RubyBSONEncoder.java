package org.jbson;

import java.io.*;
import java.math.*;
import java.nio.*;
import java.nio.charset.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.regex.*;

import org.jruby.*;
import org.jruby.exceptions.*;
import org.jruby.java.addons.*;
import org.jruby.java.proxies.*;
import org.jruby.javasupport.*;
import org.jruby.parser.*;
import org.jruby.runtime.builtin.*;
import org.jruby.util.*;

import org.bson.*;
import org.bson.io.*;
import org.bson.types.*;
import static org.bson.BSON.*;

/**
 * this is meant to be pooled or cached
 * there is some per instance memory for string conversion, etc...
 */
@SuppressWarnings("unchecked")
public class RubyBSONEncoder extends BasicBSONEncoder {

    private static final boolean DEBUG = false;

    private Ruby _runtime;

    private RubyModule _rbclsByteBuffer;
    private RubyModule _rbclsInvalidDocument;
    private RubyModule _rbclsInvalidKeyName;
    private RubyModule _rbclsRangeError;
    private RubySymbol _idAsSym;
    private RubyString _idAsString;
    private RubyString _tfAsString;

    private boolean _check_keys;
    private boolean _move_id;

    private static final int DEFAULT_MAX_BSON_SIZE = 4 * 1024 * 1024;
    private static int _max_bson_size              = DEFAULT_MAX_BSON_SIZE;
    private static final int BIT_SIZE              = 64;
    private static final long MAX                  = (1L << (BIT_SIZE - 1)) - 1;
    private static final BigInteger LONG_MAX       = BigInteger.valueOf(MAX);
    private static final BigInteger LONG_MIN       = BigInteger.valueOf(-MAX - 1);


    public RubyBSONEncoder(Ruby runtime, boolean check_keys, boolean move_id, int max_bson_size){
        _max_bson_size        = max_bson_size;
        _check_keys           = check_keys;
        _move_id              = move_id;
        _runtime              = runtime;
        _rbclsByteBuffer      = runtime.getClassFromPath( "BSON::ByteBuffer" );
        _rbclsInvalidDocument = runtime.getClassFromPath( "BSON::InvalidDocument" );
        _rbclsInvalidKeyName  = runtime.getClassFromPath( "BSON::InvalidKeyName" );
        _rbclsRangeError      = runtime.getClassFromPath( "RangeError" );
        _idAsSym              = runtime.newSymbol( "_id" );
        _tfAsString           = runtime.newString( "_transientFields" );

        if(_idAsString == null) {
            _idAsString = _runtime.newString( "_id" );
        }
    }

    public static RubyFixnum max_bson_size(RubyObject obj) {
        Ruby _run = obj.getRuntime();
        return _run.newFixnum(_max_bson_size);
    }

    public static RubyFixnum update_max_bson_size(RubyObject obj, RubyObject conn) {
        Ruby _run = obj.getRuntime();
        _max_bson_size = ((Long)JavaEmbedUtils.invokeMethod( _run, conn, "max_bson_size",
                new Object[] {}, Object.class)).intValue();
        return _run.newFixnum(_max_bson_size);
    }

    public RubyString encode( Object arg ) {
        RubyHash o = (RubyHash)arg;
        BasicOutputBuffer buf = new BasicOutputBuffer();
        set( buf );
        putObject( o );
        done();

        return RubyString.newString(_runtime, buf.toByteArray());
    }

    public void set( OutputBuffer out ) {
        if ( _buf != null ) {
            done();
            throw new IllegalStateException( "Error! The output buffer is in an illegal state." );
        }

        _buf = out;
    }

    public void done(){
        _buf = null;
    }

    protected boolean handleSpecialObjects( String name , RubyObject o ){
        return false;
    }

    public int putObject( RubyObject o ) {
        return putObject( null, o );
    }

    int putObject( String name , RubyObject o ){
        if ( o == null )
            throw new NullPointerException( "Error! Null objects are not permitted." );

        final int start = _buf.getPosition();

        byte myType = OBJECT;
        if ( o instanceof RubyArray )
            myType = ARRAY;

        if ( handleSpecialObjects( name , o ) )
            return _buf.getPosition() - start;

        if ( name != null ){
            _put( myType , name );
        }

        final int sizePos = _buf.getPosition();
        _buf.writeInt( 0 );

        List transientFields = null;
        boolean rewriteID = _move_id && ( myType == OBJECT && name == null );

        if ( myType == OBJECT ) {

            if ( rewriteID ) {

                if (  _rbHashHasKey( (RubyHash)o, "_id" ) ) {
                    _putObjectField( "_id" , _rbHashGet( (RubyHash)o, _idAsString ) );
                }
                else if ( ( _rbHashHasKey( (RubyHash)o, _idAsSym )) ) {
                    _putObjectField( "_id" , _rbHashGet( (RubyHash)o, _idAsSym ) );
                }

                RubyObject temp = (RubyObject)_rbHashGet( (RubyHash)o, _tfAsString );
                if ( temp instanceof RubyArray )
                    transientFields = (RubyArray)temp;
            }
            else {
                if (  _rbHashHasKey( (RubyHash)o, "_id" ) && _rbHashHasKey( (RubyHash)o, _idAsSym ) ) {
                    ((RubyHash)o).fastDelete(_idAsSym);
                }


            }

            RubyArray keys = (RubyArray)JavaEmbedUtils.invokeMethod( _runtime, o , "keys" , new Object[] {}
                    , Object.class);

            for (Iterator<RubyObject> i = keys.iterator(); i.hasNext(); ) {

                Object hashKey = i.next();

                String str = "";
                if( hashKey instanceof String) {
                    str = hashKey.toString();
                }
                else if (hashKey instanceof RubyString) {
                    str = ((RubyString)hashKey).asJavaString();
                }
                else if (hashKey instanceof RubySymbol) {
                    str = ((RubySymbol)hashKey).asJavaString();
                }

                if ( rewriteID && str.equals( "_id" ) )
                    continue;

                RubyObject val = (RubyObject)_rbHashGet( (RubyHash)o, hashKey );
                _putObjectField( str , (Object)val );
            }
        }

        if ( _buf.size() > _max_bson_size ) {
            _rbRaise( (RubyClass)_rbclsInvalidDocument,
                    "Error! Document is too large (" + _buf.size() + "). BSON documents are limited to " +
                            _max_bson_size + " bytes." );
        }

        _buf.write( EOO );

        _buf.writeInt( sizePos , _buf.getPosition() - sizePos );
        return _buf.getPosition() - start;
    }

    protected void _putObjectField( String name , Object val ) {
        if( _check_keys )
            testValidKey( name );
        else
            testNull( name );

        if ( name.equals( "_transientFields" ) )
            return;

        if ( DEBUG ) {
            System.out.println( "\t put thing : " + name );
            if( val == null )
                System.out.println( "\t class : null value" );
            else
                System.out.println( "\t class : " + val.getClass().getName() );
        }

        if ( name.equals( "$where") && val instanceof String ) {
            _put( CODE , name );
            _putValueString( val.toString() );
            return;
        }

        if ( val instanceof String )
            putString(name, val.toString() );

        else if ( val instanceof Number ) {
            if ( ( val instanceof Float ) || ( val instanceof Double ) ) {
                _put( NUMBER , name );
                _buf.writeDouble( ((Number)val).doubleValue() );
            }
            else {
                long longVal = ((Number)val).longValue();
                if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE) {
                    _put( NUMBER_INT , name );
                    _buf.writeInt( (int)longVal );
                }
                else {
                    _put( NUMBER_LONG , name );
                    _buf.writeLong( longVal );
                }
            }
        }

        else if ( val instanceof Boolean )
            putBoolean(name, (Boolean)val);

        else if ( val instanceof Map )
            putMap( name , (Map)val );

        else if ( val instanceof Iterable)
            putIterable( name , (Iterable)val );

        else if ( val instanceof Date )
            putDate( name , ((Date)val).getTime() );

        else if ( val instanceof byte[] )
            putBinary( name , (byte[])val );

        else if ( val == null )
            putNull(name);

        else if ( val.getClass().isArray() )
            putIterable( name , Arrays.asList( (Object[])val ) );

        else if ( val instanceof RubyObject ) {

            if ( val instanceof RubyString ) {
                putRubyString(name, (RubyString)val);
            }

            else if (val instanceof RubySymbol) {
                putSymbol(name, new Symbol(val.toString()));
            }

            else if ( val instanceof RubyFixnum ) {
                long jval = ((RubyFixnum)val).getLongValue();

                if (jval >= Integer.MIN_VALUE && jval <= Integer.MAX_VALUE) {
                    _put( NUMBER_INT , name );
                    _buf.writeInt( (int)jval );
                }
                else {
                    _put( NUMBER_LONG , name );
                    _buf.writeLong( jval );
                }
            }

            else if ( val instanceof RubyFloat ) {
                double doubleValue = ((RubyFloat)val).getValue();

                _put( NUMBER , name );
                _buf.writeDouble( doubleValue );
            }

            else if ( val instanceof JavaProxy ) {
                Object obj = ((JavaProxy)val).getObject();
                if ( obj instanceof ArrayList ) {
                    putIterable( name, ((ArrayList)obj));
                }
                else {
                    _rbRaise( (RubyClass)_rbclsInvalidDocument,
                            "Error! Received a JavaProxy object that can't serialized as a BSON type." );
                }
            }

            else if ( val instanceof RubyNil )
                putNull(name);

            else if ( val instanceof RubyTime )
                putDate( name , ((RubyTime)val).getDateTime().getMillis() );

            else if ( val instanceof RubyBoolean )
                putBoolean(name, (Boolean)((RubyBoolean)val).toJava(Boolean.class));

            else if ( val instanceof RubyRegexp )
                putRubyRegexp(name, (RubyRegexp)val );

            else if (val instanceof RubyBignum) {
                BigInteger big = ((RubyBignum)val).getValue();
                if( big.compareTo(LONG_MAX) > 0 || big.compareTo(LONG_MIN) < 0 ) {
                    _rbRaise( (RubyClass)_rbclsRangeError , "Error! Only 8 byte integer values can be serialized." );
                }
                else {
                    long jval = big.longValue();
                    _put( NUMBER_LONG , name );
                    _buf.writeLong( jval );
                }
            }

            // This is where we handle special types defined in the Ruby BSON.
            else {
                String klass = JavaEmbedUtils.invokeMethod(_runtime, val,
                        "class", new Object[] {}, Object.class).toString();

                if( klass.equals( "BSON::ObjectId" ) ) {
                    putRubyObjectId(name, (RubyObject)val );
                }
                else if( klass.equals( "BSON::ObjectID" ) ) {
                    putRubyObjectId(name, (RubyObject)val );
                }
                else if( klass.equals( "Java::JavaUtil::ArrayList" ) ) {
                    putIterable(name, (Iterable)val );
                }
                else if ( klass.equals( "BSON::Code" ) ) {
                    putRubyCodeWScope(name, (RubyObject)val );
                }
                else if ( klass.equals( "BSON::Binary" ) ) {
                    putRubyBinary( name , (RubyObject)val );
                }
                else if ( klass.equals("BSON::MinKey") ) {
                    _put( MINKEY, name );
                }
                else if ( klass.equals("BSON::MaxKey") ) {
                    _put( MAXKEY, name );
                }
                else if ( klass.equals("BSON::Timestamp") ) {
                    putRubyTimestamp( name, (RubyObject)val );
                }
                else if ( klass.equals("BSON::DBRef") ) {
                    RubyHash ref = (RubyHash)JavaEmbedUtils.invokeMethod(_runtime, val,
                            "to_hash", new Object[] {}, Object.class);
                    putMap( name , (Map)ref );
                }
                else if ( klass.equals("Date") || klass.equals("DateTime") ||
                        klass.equals("ActiveSupport::TimeWithZone") ) {

                    _rbRaise( (RubyClass)_rbclsInvalidDocument,
                            "Error! " + klass + " is not currently supported; use a UTC Time instance instead.");
                }
                else {
                    _rbRaise( (RubyClass)_rbclsInvalidDocument,
                            "Error! Cannot serialize " + klass + " as a BSON type; " +
                                    "it either isn't supported or won't translate to BSON.");

                }
            }
        }
        else {
            String klass = JavaEmbedUtils.invokeMethod(_runtime, val,
                    "class", new Object[] {}, Object.class).toString();

            _rbRaise( (RubyClass)_rbclsInvalidDocument,
                    "Error! Cannot serialize " + klass + " as a BSON type; " +
                            "it either isn't supported or won't translate to BSON.");
        }
    }

    private void testNull(String str) {
        byte[] bytes = str.getBytes();

        for(int j = 0; j < bytes.length; j++ ) {
            if(bytes[j] == '\u0000') {
                _rbRaise( (RubyClass)_rbclsInvalidDocument, "Error! Null values are not permitted.");
            }
        }
    }

    private void testValidKey(String str) {
        byte[] bytes = str.getBytes();

        if( bytes[0] == 36 )
            _rbRaise( (RubyClass)_rbclsInvalidKeyName, "Error! The '$' character is not allowed in key name.");

        for(int j = 0; j < bytes.length; j++ ) {
            if(bytes[j] == '\u0000')
                _rbRaise( (RubyClass)_rbclsInvalidDocument, "Error! Null values are not permitted.");
            if(bytes[j] == 46)
                _rbRaise( (RubyClass)_rbclsInvalidKeyName, "Error! Key names must not contain '.' (" + str + ").");
        }
    }

    private void putIterable( String name , Iterable l ){
        _put( ARRAY , name );
        final int sizePos = _buf.getPosition();
        _buf.writeInt( 0 );

        int i=0;
        for ( Object obj: l ) {
            _putObjectField( String.valueOf( i ) , obj );
            i++;
        }


        _buf.write( EOO );
        _buf.writeInt( sizePos , _buf.getPosition() - sizePos );
    }

    private void putMap( String name , Map m ){
        _put( OBJECT , name );
        final int sizePos = _buf.getPosition();
        _buf.writeInt( 0 );

        RubyArray keys = (RubyArray)JavaEmbedUtils.invokeMethod( _runtime, m , "keys" , new Object[] {} , Object.class);

        for (Iterator<RubyObject> i = keys.iterator(); i.hasNext(); ) {

            Object hashKey = i.next();

            String str = "";
            if( hashKey instanceof String) {
                str = hashKey.toString();
            }
            else if (hashKey instanceof RubyString) {
                str = ((RubyString)hashKey).asJavaString();
            }
            else if (hashKey instanceof RubySymbol) {
                str = ((RubySymbol)hashKey).asJavaString();
            }

            RubyObject val = (RubyObject)_rbHashGet( (RubyHash)m, hashKey );
            _putObjectField( str , (Object)val );
        }

        _buf.write( EOO );
        _buf.writeInt( sizePos , _buf.getPosition() - sizePos );
    }


    protected void putNull( String name ){
        _put( NULL , name );
    }

    protected void putUndefined(String name){
        _put(UNDEFINED, name);
    }

    protected void putTimestamp(String name, BSONTimestamp ts ){
        _put( TIMESTAMP , name );
        _buf.writeInt( ts.getInc() );
        _buf.writeInt( ts.getTime() );
    }

    protected void putRubyTimestamp(String name, RubyObject ts ){
        _put( TIMESTAMP , name );

        Number inc = (Number)JavaEmbedUtils.invokeMethod(_runtime, ts,
                "increment", new Object[] {}, Object.class);
        Number sec = (Number)JavaEmbedUtils.invokeMethod(_runtime, ts,
                "seconds", new Object[] {}, Object.class);

        _buf.writeInt( (int)inc.longValue() );
        _buf.writeInt( (int)sec.longValue() );
    }

    protected void putRubyCodeWScope( String name , RubyObject code ){
        _put( CODE_W_SCOPE , name );
        int temp = _buf.getPosition();
        _buf.writeInt( 0 );

        String code_string = (String)JavaEmbedUtils.invokeMethod(_runtime, code,
                "code", new Object[] {}, Object.class);

        _putValueString( code_string );
        putObject( (RubyObject)JavaEmbedUtils.invokeMethod(_runtime, code, "scope", new Object[] {}, Object.class) );
        _buf.writeInt( temp , _buf.getPosition() - temp );
    }

    protected void putCodeWScope( String name , CodeWScope code ){
        _put( CODE_W_SCOPE , name );
        int temp = _buf.getPosition();
        _buf.writeInt( 0 );
        _putValueString( code.getCode() );
        _buf.writeInt( temp , _buf.getPosition() - temp );
    }

    protected void putBoolean( String name , Boolean b ){
        _put( BOOLEAN , name );
        _buf.write( b ? (byte)0x1 : (byte)0x0 );
    }

    protected void putDate( String name , long millis ){
        _put( DATE , name );
        _buf.writeLong( millis );
    }

    private void putRubyBinary( String name , RubyObject binary ) {
        RubyArray rarray = (RubyArray)JavaEmbedUtils.invokeMethod(_runtime,
                binary, "to_a", new Object[] {}, Object.class);
        Long rbSubtype = (Long)JavaEmbedUtils.invokeMethod(_runtime,
                binary, "subtype", new Object[] {}, Object.class);
        long subtype = rbSubtype.longValue();
        byte[] data = ra2ba( rarray );
        if ( subtype == 2 ) {
            putBinaryTypeTwo( name, data );
        }
        else {
            _put( BINARY , name );
            _buf.writeInt( data.length );
            _buf.write( (byte)subtype );
            _buf.write( data );
        }
    }

    protected void putBinaryTypeTwo( String name , byte[] data ){
        _put( BINARY , name );
        _buf.writeInt( 4 + data.length );

        _buf.write( 2 );
        _buf.writeInt( data.length );
        _buf.write( data );
    }

    protected void putBinary( String name , byte[] data ){
        _put( BINARY , name );
        _buf.writeInt( data.length );
        _buf.write( 0 );
        _buf.write( data );
    }

    protected void putBinary( String name , Binary val ){
        _put( BINARY , name );
        _buf.writeInt( val.length() );
        _buf.write( val.getType() );
        _buf.write( val.getData() );
    }

    protected void putUUID( String name , UUID val ){
        _put( BINARY , name );
        _buf.writeInt( 4 + 64*2);
        _buf.write( 3 );
        _buf.writeLong( val.getMostSignificantBits());
        _buf.writeLong( val.getLeastSignificantBits());
    }

    protected void putSymbol( String name , Symbol s ){
        _putString(name, s.getSymbol(), SYMBOL);
    }

    protected void putRubyString( String name , RubyString s ) {
        byte[] bytes = s.getBytes();
        _put( STRING , name );
        _buf.writeInt( bytes.length + 1);
        _buf.write( bytes );
        _buf.write( (byte)0 );
    }

    protected void putString(String name, String s) {
        _putString(name, s, STRING);
    }

    private void _putString( String name , String s, byte type ){
        _put( type , name );
        _putValueString( s );
    }

    private void putRubyObjectId( String name, RubyObject oid ) {
        _put( OID , name );

        RubyArray roid = (RubyArray)JavaEmbedUtils.invokeMethod(_runtime, oid,
                "data", new Object[] {}, Object.class);
        byte[] joid = ra2ba(roid);

        _buf.writeInt( convertToInt(joid, 0) );
        _buf.writeInt( convertToInt(joid, 4) );
        _buf.writeInt( convertToInt(joid, 8) );
    }

    private void putRubyRegexp( String name, RubyRegexp r ) {
        RubyString source = (RubyString)r.source();
        testNull(source.toString());

        _put( REGEX , name );
        _put( (String)((RubyString)source).toJava(String.class) );

        int regexOptions = (int)((RubyFixnum)r.options()).getLongValue();
        String options   = "";

        if( (regexOptions & ReOptions.RE_OPTION_IGNORECASE) != 0 )
            options = options.concat( "i" );

        if( (regexOptions & ReOptions.RE_OPTION_MULTILINE) != 0 ) {
            options = options.concat( "m" );
            options = options.concat( "s" );
        }

        if( (regexOptions & ReOptions.RE_OPTION_EXTENDED) != 0 )
            options = options.concat( "x" );

        _put( options );
    }

    // Ruby-based helper methods.
    // Converts four bytes from a byte array to an int
    private int convertToInt(byte[] b, int offset) {
        return ((b[offset + 3] & 0xff) << 24) | ((b[offset + 2] & 0xff) << 16) | ((b[offset + 1] & 0xff) << 8) | ((b[offset] & 0xff));
    }

    // Ruby array to byte array
    private byte[] ra2ba( RubyArray rArray ) {
        int len  = rArray.getLength();
        byte[] b = new byte[len];
        int n    = 0;

        for ( Iterator<Object> i = rArray.iterator(); i.hasNext(); ) {
            Object value = i.next();
            b[n] = (byte)((Long)value).intValue();
            n++;
        }

        return b;
    }

    // Helper method for getting a value from a Ruby hash.
    private IRubyObject _rbHashGet(RubyHash hash, Object key) {
        if (key instanceof String) {
            return hash.op_aref( _runtime.getCurrentContext(), _runtime.newString((String)key) );
        }
        else {
            return hash.op_aref( _runtime.getCurrentContext(), (RubyObject)key );
        }
    }

    // Helper method for checking whether a Ruby hash has a certain key.
    private boolean _rbHashHasKey(RubyHash hash, String key) {
        RubyBoolean b = hash.has_key_p( _runtime.newString( key ) );
        return b == _runtime.getTrue();
    }

    private boolean _rbHashHasKey(RubyHash hash, RubySymbol sym) {
        RubyBoolean b = hash.has_key_p( sym );
        return b == _runtime.getTrue();
    }

    // Helper method for setting a value in a Ruby hash.
    private IRubyObject _rbHashSet(RubyHash hash, String key, IRubyObject value) {
        return hash.op_aset( _runtime.getCurrentContext(), _runtime.newString( key ), value );
    }


    // Helper method for returning all keys from a Ruby hash.
    private RubyArray _rbHashKeys(RubyHash hash) {
        return hash.keys();
    }

    // Helper for raising a Ruby exception and aborting serialization.
    private RaiseException _rbRaise( RubyClass exceptionClass , String message ) {
        done();
        throw new RaiseException( _runtime, exceptionClass, message, true );
    }

    // ----------------------------------------------


    // Encodes the type and key.
    protected void _put( byte type , String name ){
        _buf.write( type );
        _put( name );
    }

    // Encodes the type and key without checking the validity of the key.
    protected void _putWithoutCheck( byte type , String name ){
        _buf.write( type );
        _put( name );
    }

    protected void _putValueString( String s ){
        int lenPos = _buf.getPosition();
        _buf.writeInt( 0 );
        int strLen = _put( s );
        _buf.writeInt( lenPos , strLen );
    }

    void _reset( Buffer b ){
        b.position(0);
        b.limit( b.capacity() );
    }

    // Puts as utf-8 string
    protected int _put( String str ){
        int total = 0;

        final int len = str.length();
        int pos = 0;
        while ( pos < len ){
            _reset( _stringC );
            _reset( _stringB );

            int toEncode = Math.min( _stringC.capacity() - 1, len - pos );
            _stringC.put( str , pos , pos + toEncode );
            _stringC.flip();

            CoderResult cr = _encoder.encode( _stringC , _stringB , false );

            if ( cr.isMalformed() || cr.isUnmappable() )
                throw new IllegalArgumentException( "Error! Received a malformed string." );

            if ( cr.isOverflow() )
                throw new RuntimeException( "Error! Encoder overflow cannot be enabled." );

            if ( cr.isError() )
                throw new RuntimeException( "Error! An encoder error has ocurred." );

            if ( !cr.isUnderflow() )
                throw new RuntimeException( "Error! Encoder underflow must be enabled." );

            total += _stringB.position();
            _buf.write( _stringB.array() , 0 , _stringB.position() );

            pos += toEncode;
        }

        _buf.write( (byte)0 );
        total++;

        return total;
    }

    public void writeInt( int x ){
        _buf.writeInt( x );
    }

    public void writeLong( long x ){
        _buf.writeLong( x );
    }

    public void writeCString( String s ){
        _put( s );
    }

    private OutputBuffer _buf;

    private CharBuffer _stringC = CharBuffer.wrap( new char[256 + 1] );
    private ByteBuffer _stringB = ByteBuffer.wrap( new byte[1024 + 1] );
    private CharsetEncoder _encoder = Charset.forName( "UTF-8" ).newEncoder();
}
