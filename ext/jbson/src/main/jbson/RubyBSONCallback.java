package org.jbson;

import org.jruby.*;
import org.jruby.runtime.builtin.*;
import org.jruby.runtime.*;
import org.jruby.runtime.callsite.*;
import org.jruby.util.*;

import org.jruby.javasupport.*;

import java.io.*;
import java.lang.*;
import java.util.*;
import java.util.regex.*;

import org.bson.*;
import org.bson.types.*;

public class RubyBSONCallback implements BSONCallback {

    private RubyHash _root;
    private RubyModule _rbclsOrderedHash;
    private RubyModule _rbclsObjectId;
    private RubyModule _rbclsBinary;
    private RubyModule _rbclsMinKey;
    private RubyModule _rbclsMaxKey;
    private RubyModule _rbclsTimestamp;
    private RubyModule _rbclsCode;
    private final LinkedList<RubyObject> _stack = new LinkedList<RubyObject>();
    private final LinkedList<String> _nameStack = new LinkedList<String>();
    private Ruby _runtime;

    public RubyBSONCallback(Ruby runtime) {
      _runtime          = runtime;
      _rbclsOrderedHash = runtime.getClassFromPath( "BSON::OrderedHash" );
      _rbclsBinary      = runtime.getClassFromPath( "BSON::Binary" );
      _rbclsCode        = runtime.getClassFromPath( "BSON::Code" );
      _rbclsMinKey      = runtime.getClassFromPath( "BSON::MinKey" );
      _rbclsMaxKey      = runtime.getClassFromPath( "BSON::MaxKey" );
      _rbclsTimestamp   = runtime.getClassFromPath( "BSON::Timestamp" );
      _rbclsObjectId    = runtime.getClassFromPath( "BSON::ObjectId" );
    }

    public BSONCallback createBSONCallback(){
        return new RubyBSONCallback(_runtime);
    }

    public void reset(){
        _root = null;
        _stack.clear();
        _nameStack.clear();
    }

    public RubyHash createHash() { // OrderedHash
      return (RubyHash)JavaEmbedUtils.invokeMethod(_runtime, _rbclsOrderedHash, "new",
              new Object[] { }, Object.class);
    }

    public RubyArray createArray() {
      return RubyArray.newArray(_runtime);
    }

    public RubyObject create( boolean array , List<String> path ){
        if ( array )
            return createArray();
        return createHash();
    }

    public void objectStart(){
        if ( _stack.size() > 0 ) {
            throw new IllegalStateException( "Error! An illegal state ocurred." );
        }

        _root = createHash();
        _stack.add(_root);
    }

    public void objectStart(boolean f) {
        objectStart();
    }

    public void objectStart(String key){
        RubyHash hash = createHash();

        _nameStack.addLast( key );

        RubyObject lastObject = _stack.getLast();

        if(lastObject instanceof RubyHash) {
            writeRubyHash(key, (RubyHash)lastObject, (IRubyObject)hash);
        }
        else {
            writeRubyArray(key, (RubyArray)lastObject, (IRubyObject)hash);
        }

        _stack.addLast( (RubyObject)hash );
    }

    // Note: we use []= because we're dealing with an OrderedHash, which in 1.8
    // doesn't have an internal JRuby representation.
    public void writeRubyHash(String key, RubyHash hash, IRubyObject obj) {
        RubyString rkey = _runtime.newString(key);
        JavaEmbedUtils.invokeMethod(_runtime, hash, "[]=",
          new Object[] { (IRubyObject)rkey, obj }, Object.class);
    }

        public void writeRubyArray(String key, RubyArray array, IRubyObject obj) {
        Long index = Long.parseLong(key);
        array.store(index, obj);
    }

    public void arrayStart(String key){
        RubyArray array = createArray();

        RubyObject lastObject = _stack.getLast();
        _nameStack.addLast( key );

        if(lastObject instanceof RubyHash) {
            writeRubyHash(key, (RubyHash)lastObject, (IRubyObject)array);
        }
        else {
            writeRubyArray(key, (RubyArray)lastObject, (IRubyObject)array);
        }

        _stack.addLast( (RubyObject)array );
    }

    public RubyObject objectDone(){
        RubyObject o =_stack.removeLast();
        if ( _nameStack.size() > 0 )
            _nameStack.removeLast();
        else if ( _stack.size() > 0 ) {
        throw new IllegalStateException( "Error! An illegal state ocurred." );
    }
        return o;
    }

    public void arrayStart(){
        throw new UnsupportedOperationException();
    }

    public RubyObject arrayDone(){
        return objectDone();
    }

    public void gotNull( String name ){
        _put(name, (RubyObject)_runtime.getNil());
    }

    @Deprecated
    public void gotUndefined( String name ) { }

    public void gotUUID( String name , long part1, long part2) {
      throw new UnsupportedOperationException();
    }

    public void gotCode( String name , String code ){
        RubyString code_string = _runtime.newString( code );
        Object rb_code_obj = JavaEmbedUtils.invokeMethod(_runtime, _rbclsCode,
            "new", new Object[] { code_string }, Object.class);
        _put( name , (RubyObject)rb_code_obj );
    }

    public void gotCodeWScope( String name , String code , Object scope ){
        RubyString code_string = _runtime.newString( code );

        Object rb_code_obj = JavaEmbedUtils.invokeMethod(_runtime, _rbclsCode,
            "new", new Object[] { code_string, (RubyHash)scope }, Object.class);

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
        RubyBoolean b = RubyBoolean.newBoolean( _runtime, v );
        _put(name , b);
    }

    public void gotDouble( String name , double v ){
        RubyFloat f = new RubyFloat( _runtime, v );
        _put(name , (RubyObject)f);
    }

    public void gotInt( String name , int v ){
        RubyFixnum f = new RubyFixnum( _runtime, v );
        _put(name , (RubyObject)f);
    }

    public void gotLong( String name , long v ){
        RubyFixnum f = new RubyFixnum( _runtime, v );
        _put(name , (RubyObject)f);
    }

    public void gotDate( String name , long millis ){
        RubyTime time = RubyTime.newTime(_runtime, millis).gmtime();
        _put( name , time );
    }

    public void gotRegex( String name , String pattern , String flags ){
      int f = 0;
      RegexpOptions opts = new RegexpOptions();
      ByteList b = new ByteList(pattern.getBytes());

      if(flags.contains("i")) {
        opts.setIgnorecase(true);
      }
      if(flags.contains("m")) {
        opts.setMultiline(true);
      }
      if(flags.contains("s")) {
        opts.setMultiline(true);
      }
      if(flags.contains("x")) {
        opts.setExtended(true);
      }

      _put( name , RubyRegexp.newRegexp(_runtime, b, opts) );
    }

    public void gotString( String name , String v ){
        RubyString str = RubyString.newString(_runtime, v);
        _put( name , str );
    }

    public void gotSymbol( String name , String v ){
        ByteList bytes = new ByteList(v.getBytes());
        RubySymbol symbol = _runtime.getSymbolTable().getSymbol(bytes);
        _put( name , symbol );
    }

    public void gotTimestamp( String name , int time , int inc ){
        RubyFixnum rtime = RubyFixnum.newFixnum( _runtime, time );
        RubyFixnum rinc  = RubyFixnum.newFixnum( _runtime, inc );
        RubyObject[] args = new RubyObject[2];
        args[0] = rtime;
        args[1] = rinc;

        Object result = JavaEmbedUtils.invokeMethod(_runtime, _rbclsTimestamp, "new", args, Object.class);

        _put ( name , (RubyObject)result );
    }

    public void gotObjectId( String name , ObjectId id ){
       byte[] jbytes = id.toByteArray();
       RubyArray arg = RubyArray.newArray( _runtime, 12 );
       for( int i=0; i<jbytes.length; i++) {
         arg.store( i, _runtime.newFixnum(jbytes[i] & 0xFF) );
       }
       Object[] args = new Object[] { arg };
       Object result = JavaEmbedUtils.invokeMethod(_runtime, _rbclsObjectId, "new", args, Object.class);
       _put( name, (RubyObject)result );
    }

    @Deprecated
    public void gotDBRef( String name , String ns , ObjectId id ){
        throw new UnsupportedOperationException();
    }

    private RubyArray ja2ra( byte[] b ) {
        RubyArray result = RubyArray.newArray( _runtime, b.length );

        for ( int i=0; i<b.length; i++ ) {
            result.store( i, _runtime.newFixnum(b[i]) );
        }

        return result;
    }

    @Deprecated
    public void gotBinaryArray( String name , byte[] b ) {
      throw new UnsupportedOperationException();
    }

    public void gotBinary( String name , byte type , byte[] data ){
        RubyArray a = ja2ra( data );
        Object[] args = new Object[] { a, RubyFixnum.newFixnum(_runtime, Math.abs( type )) };
        Object result = JavaEmbedUtils.invokeMethod(_runtime, _rbclsBinary, "new", args, Object.class);
        _put( name, (RubyObject)result );
    }

    protected void _put( String name , RubyObject o ){
        RubyObject current = cur();
        if(current instanceof RubyArray) {
          RubyArray a = (RubyArray)current;
          Long n = Long.parseLong(name);
          a.store(n, (IRubyObject)o);
        }
        else {
          RubyString rkey = RubyString.newString(_runtime, name);
          JavaEmbedUtils.invokeMethod(_runtime, current, "[]=",
            new Object[] { (IRubyObject)rkey, o }, Object.class);
        }
    }

    protected RubyObject cur(){
        return _stack.getLast();
    }

    public Object get(){
      return _root;
    }

    protected void setRoot(RubyHash o) {
      _root = o;
    }

    protected boolean isStackEmpty() {
      return _stack.size() < 1;
    }
}
