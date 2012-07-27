#!/usr/bin/env ruby

require 'socket'
require 'fileutils'

$debug_level = 2
STDOUT.sync = true

unless defined? Mongo
  require File.join(File.dirname(__FILE__), '..', '..', 'lib', 'mongo')
end

def debug(level, arg)
  if level <= $debug_level
    file_line = caller[0][/(.*:\d+):/, 1]
    calling_method = caller[0][/`([^']*)'/, 1]
    puts "#{file_line}:#{calling_method}:#{arg.class == String ? arg : arg.inspect}"
  end
end

module Mongo
  class Config
    DEFAULT_BASE_OPTS = { :host => 'localhost', :logpath => 'data/log', :dbpath => 'data' }
    DEFAULT_REPLICA_SET = DEFAULT_BASE_OPTS.merge( :replicas => 3 )
    DEFAULT_SHARDED_SIMPLE = DEFAULT_BASE_OPTS.merge( :shards => 2, :configs => 1, :routers => 2 )
    DEFAULT_SHARDED_REPLICA = DEFAULT_SHARDED_SIMPLE.merge( :replicas => 3 )

    SERVER_PRELUDE_KEYS = [:host, :command]
    SHARDING_OPT_KEYS = [:shards, :configs, :routers]
    REPLICA_OPT_KEYS = [:replicas]
    MONGODS_OPT_KEYS = [:mongods]
    CLUSTER_OPT_KEYS = SHARDING_OPT_KEYS + REPLICA_OPT_KEYS + MONGODS_OPT_KEYS

    DEFAULT_VERIFIES = 60
    BASE_PORT = 3000
    @@port = BASE_PORT

    def self.configdb(config)
      config[:configs].collect{|c|"#{c[:host]}:#{c[:port]}"}.join(' ')
    end

    def self.cluster(opts = DEFAULT_SHARDED_SIMPLE)
      raise "missing required option" if [:host, :dbpath].any?{|k| !opts[k]}
      config = opts.reject{|k,v| CLUSTER_OPT_KEYS.include?(k)}
      keys = SHARDING_OPT_KEYS.any?{|k| opts[k]} ? SHARDING_OPT_KEYS : nil
      keys ||= REPLICA_OPT_KEYS.any?{|k| opts[k]} ? REPLICA_OPT_KEYS : nil
      keys ||= MONGODS_OPT_KEYS
      keys.each do |key|
        config[key] = opts.fetch(key,1).times.collect do |i| #default to 1 of whatever
          server_base = key.to_s.chop
          dbpath = "#{opts[:dbpath]}/#{server_base}#{i}"
          logpath = "#{dbpath}/#{server_base}.log"
          if key == :shards && opts[:replicas]
            self.cluster(opts.reject{|k,v| SHARDING_OPT_KEYS.include?(k)}.merge(:dbpath => dbpath))
          else
            server_params = { :host => opts[:host], :port => self.get_available_port, :logpath => logpath }
            case key
              when :replicas; server_params.merge!( :command => 'mongod', :dbpath => dbpath, :replSet => File.basename(opts[:dbpath]) )
              when :configs;  server_params.merge!( :command => 'mongod', :dbpath => dbpath, :configsvr => nil )
              when :routers;  server_params.merge!( :command => 'mongos', :configdb => self.configdb(config) ) # mongos, NO dbpath
              else            server_params.merge!( :command => 'mongod', :dbpath => dbpath ) # :mongods, :shards
            end
          end
        end
      end
      config
    end

    def self.port_available?(port)
      ret = false
      socket = Socket.new(Socket::Constants::AF_INET, Socket::Constants::SOCK_STREAM, 0)
      sockaddr = Socket.sockaddr_in(port, '0.0.0.0')
      begin
        socket.bind(sockaddr)
        ret = true
      rescue Exception
      end
      socket.close
      ret
    end

    def self.get_available_port
      while true
        port = @@port
        @@port += 1
        break if port_available?(port)
      end
      port
    end

    class SysProc
      attr_reader :pid, :cmd

      def initialize(cmd = nil)
        @pid = nil
        @cmd = cmd
      end

      def start(verifies = 0)
        return @pid if running?
        begin
          @pid = fork do
            STDIN.reopen '/dev/null'
            STDOUT.reopen '/dev/null', 'a'
            STDERR.reopen STDOUT
            exec cmd # spawn(@cmd, [:in, :out, :err] => :close) #
          end
          verify(verifies) if verifies > 0
          @pid
        end
      end

      def stop
        kill
        wait
      end

      def kill
        begin
          @pid && Process.kill(3, @pid) && true
        rescue Errno::ESRCH
          false
        end
      end

      def wait
        Process.wait(@pid) if @pid
        @pid = nil
      end

      def running?
        begin
          @pid && Process.kill(0, @pid) && true
        rescue Errno::ESRCH
          false
        end
      end

      def verify(verifies = DEFAULT_VERIFIES)
        verifies.times do |i|
          return @pid if running?
          sleep 1
        end
        nil
      end
    end

    class Server < SysProc
      attr_reader :host, :port

      def initialize(cmd = nil, host = nil, port = nil)
        super(cmd)
        @host = host
        @port = port
      end
    end

    class DbServer < Server
      attr_accessor :config
      def initialize(config)
        @config = config
        dbpath = @config[:dbpath]
        [dbpath, File.dirname(@config[:logpath])].compact.each{|dir| FileUtils.mkdir_p(dir) unless File.directory?(dir) }
        command = @config[:command] || 'mongod'
        arguments = @config.reject{|k,v| SERVER_PRELUDE_KEYS.include?(k)}
        cmd = [command, arguments.collect{|k,v| ['--' + k.to_s, v ]}].flatten.join(' ')
        super(cmd, @config[:host], @config[:port])
      end

      def start(verifies = DEFAULT_VERIFIES)
        super(verifies)
        verify(verifies)
      end

      def verify(verifies = 10)
        verifies.times do |i|
          #puts "DbServer.verify - port: #{@port} iteration: #{i}"
          begin
            raise Mongo::ConnectionFailure unless running?
            Mongo::Connection.new(@host, @port).close
            #puts "DbServer.verified via connection - port: #{@port} iteration: #{i}"
            return @pid
          rescue Mongo::ConnectionFailure
            sleep 1
          end
        end
        raise Mongo::ConnectionFailure, "DbServer.start verification via connection failed - port: #{@port}"
      end

    end

    class ClusterManager
      attr_reader :config
      def initialize(config)
        @config = config
        @servers = {}
        Mongo::Config::CLUSTER_OPT_KEYS.each do |key|
          @servers[key] = @config[key].collect{|conf| DbServer.new(conf)} if @config[key]
        end
      end

      def servers(key = nil)
        @servers.collect{|k,v| (!key || key == k) ? v : nil}.flatten.compact
      end

      def command( cmd_servers, db_name, cmd, opts = {} )
        ret = []
        cmd = cmd.class == Array ? cmd : [ cmd ]
        debug 3, "ClusterManager.command cmd:#{cmd.inspect}"
        cmd_servers = cmd_servers.class == Array ? cmd_servers : [cmd_servers]
        cmd_servers.each do |cmd_server|
          debug 3, cmd_server.inspect
          conn = Mongo::Connection.new(cmd_server[:host], cmd_server[:port])
          cmd.each do |c|
            debug 3,  "ClusterManager.command c:#{c.inspect}"
            response = conn[db_name].command( c, opts )
            debug 3,  "ClusterManager.command response:#{response.inspect}"
            raise Mongo::OperationFailure, "c:#{c.inspect} opts:#{opts.inspect} failed" unless response["ok"] == 1.0 || opts.fetch(:check_response, true) == false
            ret << response
          end
          conn.close
        end
        debug 3, "command ret:#{ret.inspect}"
        ret.size == 1 ? ret.first : ret
      end

      def repl_set_get_status
        command( @config[:replicas].first, 'admin', { :replSetGetStatus => 1 }, {:check_response => false } )
      end

      def repl_set_initiate( cfg = nil )
        cfg ||= {
            :_id => @config[:replicas].first[:replSet],
            :members => @config[:replicas].each_with_index.collect{|s, i| { :_id => i, :host => "#{s[:host]}:#{s[:port]}" } },
        }
        command( @config[:replicas].first, 'admin', { :replSetInitiate => cfg } )
      end

      def repl_set_startup
        response = nil
        60.times do |i|
          break if (response = repl_set_get_status)['ok'] == 1.0
          sleep 1
        end
        raise Mongo::OperationFailure, "replSet startup failed - status: #{repsonse.inspect}" unless response && response['ok'] == 1.0
        response
      end

      def mongos_seeds
        @config[:routers].collect{|router| "#{router[:host]}:#{router[:port]}"}
      end

      def ismaster
        command( @config[:routers], 'admin', { :ismaster => 1 } )
      end

      def addshards(shards = @config[:shards])
        command( @config[:routers].first, 'admin', Array(shards).collect{|s| { :addshard => "#{s[:host]}:#{s[:port]}" } } )
      end

      def listshards
        command( @config[:routers].first, 'admin', { :listshards => 1 } )
      end

      def enablesharding( dbname )
        command( @config[:routers].first, 'admin', { :enablesharding => dbname } )
      end

      def shardcollection( namespace, key, unique = false )
        command( @config[:routers].first, 'admin', { :shardcollection => namespace, :key => key, :unique => unique } )
      end

      def mongos_discover # can also do @config[:routers] find but only want mongos for connections
        (@config[:configs]).collect do |cmd_server|
          conn = Mongo::Connection.new(cmd_server[:host], cmd_server[:port])
          result = conn['config']['mongos'].find.to_a
          conn.close
          result
        end
      end

      def start
        servers.each{|server| server.start}
        # TODO - sharded replica sets - pending
        if @config[:replicas]
          repl_set_initiate if repl_set_get_status['startupStatus'] == 3
          repl_set_startup
        end
        if @config[:routers]
          addshards if listshards['shards'].size == 0
        end
        self
      end

      def stop
        servers.each{|server| server.stop}
        self
      end

      def clobber
        system "rm -fr #{@config[:dbpath]}"
        self
      end
    end

  end
end
