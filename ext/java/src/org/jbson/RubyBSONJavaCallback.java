// BSON Callback
// RubyBSONCallback.java
package org.jbson;

import org.jruby.*;
import org.jruby.util.ByteList;
import org.jruby.RubyString;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.Block;
import org.jruby.runtime.CallType;
import org.jruby.runtime.callsite.CacheEntry;

import org.jruby.javasupport.JavaEmbedUtils;
import org.jruby.javasupport.JavaUtil;

import org.jruby.parser.ReOptions;

import org.jruby.RubyArray;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import org.bson.*;
import org.bson.types.*;

public class RubyBSONJavaCallback implements BSONCallback {

    private LinkedHashMap _root;
    private RubyModule _rbclsOrderedHash;
    private RubyModule _rbclsObjectId;
    private RubyModule _rbclsBinary;
    private RubyModule _rbclsMinKey;
    private RubyModule _rbclsMaxKey;
    private RubyModule _rbclsDBRef;
    private RubyModule _rbclsCode;
    private final LinkedList<Object> _stack = new LinkedList<Object>();
    private final LinkedList<String> _nameStack = new LinkedList<String>();
    private Ruby _runtime;
    static final HashMap<Ruby, HashMap> _runtimeCache = new HashMap<Ruby, HashMap>();

    public RubyBSONJavaCallback(Ruby runtime) {
      _runtime          = runtime;
      _rbclsOrderedHash = _lookupConstant( _runtime, "BSON::OrderedHash" );
      _rbclsBinary      = _lookupConstant( _runtime, "BSON::Binary" );
      _rbclsDBRef       = _lookupConstant( _runtime, "BSON::DBRef" );
      _rbclsCode        = _lookupConstant( _runtime, "BSON::Code" );
      _rbclsMinKey      = _lookupConstant( _runtime, "BSON::MinKey" );
      _rbclsMaxKey      = _lookupConstant( _runtime, "BSON::MaxKey" );
      _rbclsObjectId    = _lookupConstant( _runtime, "BSON::ObjectId");
    }

    public BSONCallback createBSONCallback(){
        return new RubyBSONCallback(_runtime);
    }

    public void reset(){
        _root = null;
        _stack.clear();
        _nameStack.clear();
    }

    public Object create( boolean array , List<String> path ){
        if ( array )
            return new ArrayList<Object>();
        return new LinkedHashMap();
    }

    public void objectStart(){
        if ( _stack.size() > 0 ) {
            throw new IllegalStateException( "something is wrong" );
        }

        _root = new LinkedHashMap();
        _stack.add(_root);
    }

    public void objectStart(boolean f) {
        objectStart();
    }

    public void objectStart(String key){
        LinkedHashMap hash = new LinkedHashMap();

        _nameStack.addLast( key );

        Object lastObject = _stack.getLast();

        // Yes, this is a bit hacky.
        if(lastObject instanceof LinkedHashMap) {
            ((LinkedHashMap)hash).put( key, lastObject );
        }
        else {
            ((ArrayList)lastObject).add(Integer.parseInt(key), hash);
        }

        _stack.addLast( hash );
    }

    public void arrayStart(String key){
        ArrayList<Object> array = new ArrayList<Object>();

        Object lastObject = _stack.getLast();
        _nameStack.addLast( key );

        if(lastObject instanceof LinkedHashMap) {
            ((LinkedHashMap)lastObject).put( key, lastObject );
        }
        else {
            ((ArrayList)lastObject).add(Integer.parseInt(key), array);
        }

        _stack.addLast( array );
    }

    public Object objectDone(){
        Object o =_stack.removeLast();
        if ( _nameStack.size() > 0 )
            _nameStack.removeLast();
        else if ( _stack.size() > 0 ) {
        throw new IllegalStateException( "something is wrong" );
    }
        return o;
    }

    // Not used by Ruby decoder
    public void arrayStart(){
    }

    public Object arrayDone(){
        return objectDone();
    }

    public void gotNull( String name ){
        _put(name, null);
    }

    // Undefined should be represented as a lack of key / value.
    public void gotUndefined( String name ){
    }

    // TODO: Handle this
    public void gotUUID( String name , long part1, long part2) {
        //_put( name , new UUID(part1, part2) );
    }

    public void gotCode( String name , String code ){
        Object rb_code_obj = JavaEmbedUtils.invokeMethod(_runtime, _rbclsCode,
            "new", new Object[] { code }, Object.class);
        _put( name , (RubyObject)rb_code_obj );
    }

    public void gotCodeWScope( String name , String code , Object scope ){
        Object rb_code_obj = JavaEmbedUtils.invokeMethod(_runtime, _rbclsCode,
            "new", new Object[] { code, (RubyHash)scope }, Object.class);

        _put( name , (RubyObject)rb_code_obj );
    }

    public void gotMinKey( String name ){
        Object minkey = JavaEmbedUtils.invokeMethod(_runtime, _rbclsMinKey, "new", new Object[] {}, Object.class);

        _put( name, (RubyObject)minkey);
    }

    public void gotMaxKey( String name ){
        Object maxkey = JavaEmbedUtils.invokeMethod(_runtime, _rbclsMaxKey, "new", new Object[] {}, Object.class);

        _put( name, (RubyObject)maxkey);
    }

    public void gotBoolean( String name , boolean v ){
        _put(name , v);
    }

    public void gotDouble( String name , double v ){
        _put(name , v);
    }

    public void gotInt( String name , int v ){
        _put(name , v);
    }

    public void gotLong( String name , long v ){
        _put(name , v);
    }

    public void gotDate( String name , long millis ){
        RubyTime time = RubyTime.newTime(_runtime, millis).gmtime();
        _put( name , time );
    }

    public void gotRegex( String name , String pattern , String flags ){
      int f = 0;
      ByteList b = new ByteList(pattern.getBytes());

      if(flags.contains("i")) {
        f = f | ReOptions.RE_OPTION_IGNORECASE;
      }
      if(flags.contains("m")) {
        f = f | ReOptions.RE_OPTION_MULTILINE;
      }
      if(flags.contains("x")) {
        f = f | ReOptions.RE_OPTION_EXTENDED;
      }

      _put( name , RubyRegexp.newRegexp(_runtime, b, f) );
    }

    public void gotString( String name , String v ){
        _put( name , v );
    }

    public void gotSymbol( String name , String v ){
        ByteList bytes = new ByteList(v.getBytes());
        RubySymbol symbol = _runtime.getSymbolTable().getSymbol(bytes);
        _put( name , symbol );
    }

    // Timestamp is currently rendered in Ruby as a two-element array.
    public void gotTimestamp( String name , int time , int inc ){
        RubyFixnum rtime = RubyFixnum.newFixnum( _runtime, time );
        RubyFixnum rinc  = RubyFixnum.newFixnum( _runtime, inc );
        RubyObject[] args = new RubyObject[2];
        args[0] = rinc;
        args[1] = rtime;

        RubyArray result = RubyArray.newArray( _runtime, args );

        _put ( name , result );
    }

    // TODO: Optimize this. Incredibly slow.
    public void gotObjectId( String name , ObjectId id ){
       IRubyObject arg = (IRubyObject)RubyString.newString(_runtime, id.toString());
      // //System.out.println(id.toByteArray().length);
      // byte[] b = id.toByteArray();
      // RubyArray a = _runtime.newArray();
      // for(int i=0; i < b.length; i++) {
      //   System.out.println(b[i]);
      //   a.append(_runtime.newFixnum(b[i]));
      // }
       Object[] args = new Object[] { arg };

       Object result = JavaEmbedUtils.invokeMethod(_runtime, _rbclsObjectId, "from_string", args, Object.class);

        _put( name, (RubyObject)result );
    }

    // DBRef has been deprecated in the Java driver. We're going to follow suit.
    public void gotDBRef( String name , String ns , ObjectId id ){
    }

    // TODO: I know that this is horrible. To be optimized.
    // This conversion can probably be avoided altogether.
    // Used only for GridFS.
    private RubyArray ja2ra( byte[] b ) {
        RubyArray result = RubyArray.newArray( _runtime, b.length );
        
        for ( int i=0; i<b.length; i++ ) {
            result.aset( RubyNumeric.dbl2num( _runtime, (double)i ), RubyNumeric.dbl2num( _runtime, (double)b[i] ) );
        }

        return result;
    }

    public void gotBinaryArray( String name , byte[] b ) {
        RubyArray a = ja2ra( b );

        Object[] args = new Object[] { a, 2 };

        Object result = JavaEmbedUtils.invokeMethod(_runtime, _rbclsBinary, "new", args, Object.class);

        _put( name, (RubyObject)result );
    }

    // TODO: fix abs stuff here. some kind of bad type issue
    public void gotBinary( String name , byte type , byte[] data ){
        RubyArray a = ja2ra( data );

        Object[] args = new Object[] { a, RubyFixnum.newFixnum(_runtime, Math.abs( type )) };

        Object result = JavaEmbedUtils.invokeMethod(_runtime, _rbclsBinary, "new", args, Object.class);

        _put( name, (RubyObject)result );
    }

    protected void _put( String name , Object o ){
        Object current = cur();
        if(current instanceof ArrayList) {
          ((ArrayList)current).add(Integer.parseInt(name), (Object)o);
          //RubyArray a = (RubyArray)current;
          //Long n = Long.parseLong(name);
          //RubyFixnum index = new RubyFixnum(_runtime, n);
          //a.aset((IRubyObject)index, o);
        }
        else {
          ((LinkedHashMap)current).put( name, o );
        }
    }
    
    protected Object cur(){
        return _stack.getLast();
    }
    
    public Object get(){
      return _root;
    }

//    protected void setRoot(LinkedHashMap o) {
//      _root = o;
//    }
//
    protected boolean isStackEmpty() {
      return _stack.size() < 1;
    }

    static final HashMap<String, Object> _getRuntimeCache(Ruby runtime) {
      // each JRuby runtime may have different objects for these constants,
      // so cache them separately for each runtime
      @SuppressWarnings("unchecked") // aargh! Java!
      HashMap<String, Object> cache = _runtimeCache.get( runtime );

      if(cache == null) {
        cache = new HashMap<String, Object>();
        _runtimeCache.put( runtime, cache );
      }
      return cache;
    }

    static final RubyModule _lookupConstant(Ruby runtime, String name)
    {
      HashMap<String, Object> cache = _getRuntimeCache( runtime );
      RubyModule module = (RubyModule) cache.get( name );

      if(module == null && !cache.containsKey( name )) {
        module = runtime.getClassFromPath( name );
        cache.put( (String)name, (Object)module );
      }
      return module;
    }
}
