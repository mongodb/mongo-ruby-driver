#!/usr/bin/env ruby
require 'socket'
require 'fileutils'
require 'mongo'
require 'sfl'

$debug_level = 2
STDOUT.sync = true

def debug(level, arg)
  if level <= $debug_level
    file_line = caller[0][/(.*:\d+):/, 1]
    calling_method = caller[0][/`([^']*)'/, 1]
    puts "#{file_line}:#{calling_method}:#{arg.class == String ? arg : arg.inspect}"
  end
end

#
# Design Notes
#     Configuration and Cluster Management are modularized with the concept that the Cluster Manager
#     can be supplied with any configuration to run.
#     A configuration can be edited, modified, copied into a test file, and supplied to a cluster manager
#     as a parameter.
#
module Mongo
  class Config
    DEFAULT_BASE_OPTS = { :host => 'localhost', :dbpath => 'data', :logpath => 'data/log' }
    DEFAULT_REPLICA_SET = DEFAULT_BASE_OPTS.merge( :replicas => 3, :arbiters => 0 )
    DEFAULT_SHARDED_SIMPLE = DEFAULT_BASE_OPTS.merge( :shards => 2, :configs => 1, :routers => 4 )
    DEFAULT_SHARDED_REPLICA = DEFAULT_SHARDED_SIMPLE.merge( :replicas => 3, :arbiters => 0)

    IGNORE_KEYS = [:host, :command, :_id]
    SHARDING_OPT_KEYS = [:shards, :configs, :routers]
    REPLICA_OPT_KEYS = [:replicas, :arbiters]
    MONGODS_OPT_KEYS = [:mongods]
    CLUSTER_OPT_KEYS = SHARDING_OPT_KEYS + REPLICA_OPT_KEYS + MONGODS_OPT_KEYS

    FLAGS = [:noprealloc, :smallfiles, :logappend, :configsvr, :shardsvr, :quiet, :fastsync, :auth]

    DEFAULT_VERIFIES = 60
    BASE_PORT = 3000
    @@port = BASE_PORT

    def self.configdb(config)
      config[:configs].collect{|c|"#{c[:host]}:#{c[:port]}"}.join(' ')
    end

    def self.cluster(opts = DEFAULT_SHARDED_SIMPLE)
      raise "missing required option" if [:host, :dbpath].any?{|k| !opts[k]}

      config = opts.reject {|k,v| CLUSTER_OPT_KEYS.include?(k)}
      kinds = CLUSTER_OPT_KEYS.select{|key| opts.has_key?(key)} # order is significant

      replica_count = 0

      kinds.each do |kind|
        config[kind] = opts.fetch(kind,1).times.collect do |i| #default to 1 of whatever
          if kind == :shards && opts[:replicas]
            self.cluster(opts.reject{|k,v| SHARDING_OPT_KEYS.include?(k)}.merge(:dbpath => path))
          else
            node = case kind
              when :replicas
                make_replica(opts, replica_count)
              when :arbiters
                make_replica(opts, replica_count)
              when :configs
                make_config(opts)
              when :routers
                make_router(config, opts)
              else
                make_mongod(kind, opts)
            end

            replica_count += 1 if [:replicas, :arbiters].member?(kind)
            node
          end
        end
      end
      config
    end

    def self.make_mongo(kind, opts)
      dbpath  = opts[:dbpath]
      port    = self.get_available_port
      path    = "#{dbpath}/#{kind}-#{port}"
      logpath = "#{path}/#{kind}.log"

      { :host      => opts[:host],
        :port      => port,
        :logpath   => logpath,
        :logappend => true }
    end

    def self.make_mongod(kind, opts)
      params = make_mongo('mongods', opts)

      mongod = ENV['MONGOD'] || 'mongod'
      path   = File.dirname(params[:logpath])

      noprealloc = opts[:noprealloc] || true
      smallfiles = opts[:smallfiles] || true
      quiet      = opts[:quiet]      || true
      fast_sync  = opts[:fastsync]   || false
      auth       = opts[:auth]       || true

      params.merge(:command    => mongod,
                   :dbpath     => path,
                   :smallfiles => smallfiles,
                   :noprealloc => noprealloc,
                   :quiet      => quiet,
                   :fastsync   => fast_sync,
                   :auth       => auth)
    end

    def self.make_replica(opts, id)
      params     = make_mongod('replicas', opts)

      replSet    = opts[:replSet]    || 'ruby-driver-test'
      oplogSize  = opts[:oplog_size] || 5
      keyFile    = opts[:key_file]   || '/test/tools/keyfile.txt'

      keyFile    = Dir.pwd << keyFile
      system "chmod 600 #{keyFile}"

      params.merge(:_id       => id,
                   :replSet   => replSet,
                   :oplogSize => oplogSize,
                   :keyFile   => keyFile)
    end

    def self.make_config(opts)
      params = make_mongod('configs', opts)
      params.merge(:configsvr => nil)
    end

    def self.make_router(config, opts)
      params = make_mongo('routers', opts)
      mongos = ENV['MONGOS'] || 'mongos'

      params.merge(
        :command => mongos,
        :configdb => self.configdb(config)
      )
    end

    def self.port_available?(port)
      ret = false
      socket = Socket.new(Socket::Constants::AF_INET, Socket::Constants::SOCK_STREAM, 0)
      socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, 1)
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

      def clear_zombie
        if @pid
          begin
            pid = Process.waitpid(@pid, Process::WNOHANG)
          rescue Errno::ECHILD
            # JVM might have already reaped the exit status
          end
          @pid = nil if pid && pid > 0
        end
      end

      def start(verifies = 0)
        clear_zombie
        return @pid if running?
        begin
          # redirection not supported in jruby
          if defined?(RUBY_ENGINE) && RUBY_ENGINE == 'jruby'
            @pid = Process.spawn(*@cmd)
          else
            cmd_and_opts = [@cmd, {:out => '/dev/null'}].flatten
            @pid = Process.spawn(*cmd_and_opts)
          end
          verify(verifies) if verifies > 0
          @pid
        end
      end

      def stop
        kill
        wait
      end

      def kill(signal_no = 2)
        begin
          @pid && Process.kill(signal_no, @pid) && true
        rescue Errno::ESRCH
          false
        end
        # cleanup lock if unclean shutdown
        begin
          File.delete(File.join(@config[:dbpath], 'mongod.lock')) if @config[:dbpath]
        rescue Errno::ENOENT
        end
      end

      def wait
        begin
          Process.waitpid(@pid) if @pid
        rescue Errno::ECHILD
          # JVM might have already reaped the exit status
        end
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

      def host_port
        [@host, @port].join(':')
      end

      def host_port_a # for old format
        [@host, @port]
      end
    end

    class DbServer < Server
      attr_accessor :config

      def initialize(config)
        @config = config
        dbpath = @config[:dbpath]
        [dbpath, File.dirname(@config[:logpath])].compact.each{|dir| FileUtils.mkdir_p(dir) unless File.directory?(dir) }
        command = @config[:command] || 'mongod'
        params = @config.reject{|k,v| IGNORE_KEYS.include?(k)}
        arguments = params.sort{|a, b| a[0].to_s <=> b[0].to_s}.collect do |arg, value| # sort block is needed for 1.8.7 which lacks Symbol#<=>
          argument = '--' + arg.to_s
          if FLAGS.member?(arg) && value == true
            [argument]
          elsif !FLAGS.member?(arg)
            [argument, value.to_s]
          end
        end
        cmd = [command, arguments].flatten.compact
        super(cmd, @config[:host], @config[:port])
      end

      def start(verifies = DEFAULT_VERIFIES)
        super(verifies)
        verify(verifies)
      end

      def verify(verifies = 600)
        verifies.times do |i|
          #puts "DbServer.verify via connection probe - port:#{@port.inspect} iteration:#{i} @pid:#{@pid.inspect} kill:#{Process.kill(0, @pid).inspect} running?:#{running?.inspect} cmd:#{cmd.inspect}"
          begin
            raise Mongo::ConnectionFailure unless running?
            Mongo::MongoClient.new(@host, @port).close
            #puts "DbServer.verified via connection - port: #{@port} iteration: #{i}"
            return @pid
          rescue Mongo::ConnectionFailure
            sleep 1
          end
        end
        system "ps -fp #{@pid}; cat #{@config[:logpath]}"
        raise Mongo::ConnectionFailure, "DbServer.start verify via connection probe failed - port:#{@port.inspect} @pid:#{@pid.inspect} kill:#{Process.kill(0, @pid).inspect} running?:#{running?.inspect} cmd:#{cmd.inspect}"
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
          cmd_server = cmd_server.config if cmd_server.is_a?(DbServer)
          client = Mongo::MongoClient.new(cmd_server[:host], cmd_server[:port])
          cmd.each do |c|
            debug 3,  "ClusterManager.command c:#{c.inspect}"
            response = client[db_name].command( c, opts )
            debug 3,  "ClusterManager.command response:#{response.inspect}"
            raise Mongo::OperationFailure, "c:#{c.inspect} opts:#{opts.inspect} failed" unless response["ok"] == 1.0 || opts.fetch(:check_response, true) == false
            ret << response
          end
          client.close
        end
        debug 3, "command ret:#{ret.inspect}"
        ret.size == 1 ? ret.first : ret
      end

      def repl_set_get_status
        command( @config[:replicas], 'admin', { :replSetGetStatus => 1 }, {:check_response => false } )
      end

      def repl_set_get_config
        host, port = primary_name.split(":")
        client = Mongo::MongoClient.new(host, port)
        client['local']['system.replset'].find_one
      end

      def repl_set_config
        members = []
        @config[:replicas].each{|s| members << { :_id => s[:_id], :host => "#{s[:host]}:#{s[:port]}", :tags => { :node => s[:_id].to_s } } }
        @config[:arbiters].each{|s| members << { :_id => s[:_id], :host => "#{s[:host]}:#{s[:port]}", :arbiterOnly => true } }
        {
          :_id => @config[:replicas].first[:replSet],
          :members => members
        }
      end

      def repl_set_initiate( cfg = nil )
        command( @config[:replicas].first, 'admin', { :replSetInitiate => cfg || repl_set_config } )
      end

      def repl_set_startup
        states = nil
        60.times do
          states = repl_set_get_status.zip(repl_set_is_master)
          healthy = states.all? do |status, is_master|
            members = status['members']
            if status['ok'] == 1.0 && members.collect{|m| m['state']}.all?{|state| [1,2,7].index(state)}
              members.any?{|m| m['state'] == 1} &&
                case status['myState']
                when 1
                  is_master['ismaster'] == true && is_master['secondary'] == false
                when 2
                  is_master['ismaster'] == false && is_master['secondary'] == true
                when 7
                  is_master['ismaster'] == false && is_master['secondary'] == false
                end
            end
          end
          return true if healthy
          sleep(1)
        end
        raise Mongo::OperationFailure, "replSet startup failed - status: #{states.inspect}"
      end

      def repl_set_seeds
        @config[:replicas].collect{|node| "#{node[:host]}:#{node[:port]}"}
      end

      def repl_set_seeds_old
        @config[:replicas].collect{|node| [node[:host], node[:port]]}
      end

      def repl_set_seeds_uri
        repl_set_seeds.join(',')
      end

      def repl_set_name
        @config[:replicas].first[:replSet]
      end

      def member_names_by_state(state)
        states = Array(state)
        status = repl_set_get_status.first
        status['members'].find_all{|member| states.index(member['state']) }.collect{|member| member['name']}
      end

      def primary_name
        member_names_by_state(1).first
      end

      def secondary_names
        member_names_by_state(2)
      end

      def replica_names
        member_names_by_state([1,2])
      end

      def arbiter_names
        member_names_by_state(7)
      end

      def members_by_name(names)
        names.collect do |name|
          member_by_name(name)
        end.compact
      end

      def member_by_name(name)
        servers.find{|server| server.host_port == name}
      end

      def primary
        members_by_name([primary_name]).first
      end

      def secondaries
        members_by_name(secondary_names)
      end

      def stop_primary
        primary.stop
      end

      def stop_secondary
        secondaries[rand(secondaries.length)].stop
      end

      def replicas
        members_by_name(replica_names)
      end

      def arbiters
        members_by_name(arbiter_names)
      end

      def config_names_by_kind(kind)
        @config[kind].collect{|conf| "#{conf[:host]}:#{conf[:port]}"}
      end

      def shards
        members_by_name(config_names_by_kind(:shards))
      end

      def repl_set_reconfig(new_config)
        new_config['version'] = repl_set_get_config['version'] + 1
        command( primary, 'admin', { :replSetReconfig => new_config } )
        repl_set_startup
      end

      def repl_set_remove_node(state = [1,2])
        names = member_names_by_state(state)
        name = names[rand(names.length)]

        @config[:replicas].delete_if{|node| "#{node[:host]}:#{node[:port]}" == name}
        repl_set_reconfig(repl_set_config)
      end

      def repl_set_add_node
      end

      def configs
        members_by_name(config_names_by_kind(:configs))
      end

      def routers
        members_by_name(config_names_by_kind(:routers))
      end

      def mongos_seeds
        config_names_by_kind(:routers)
      end

      def ismaster(servers)
        command( servers, 'admin', { :ismaster => 1 } )
      end

      def sharded_cluster_is_master
        ismaster(@config[:routers])
      end

      def repl_set_is_master
        ismaster(@config[:replicas])
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
          client = Mongo::MongoClient.new(cmd_server[:host], cmd_server[:port])
          result = client['config']['mongos'].find.to_a
          client.close
          result
        end
      end

      def start
        # Must start configs before mongos -- hash order not guaranteed on 1.8.X
        servers(:configs).each{|server| server.start}
        servers.each{|server| server.start}
        # TODO - sharded replica sets - pending
        if @config[:replicas]
          repl_set_initiate if repl_set_get_status.first['startupStatus'] == 3
          repl_set_startup
        end
        if @config[:routers]
          addshards if listshards['shards'].size == 0
        end
        self
      end
      alias :restart :start

      def stop
        servers.each{|server| server.stop}
        self
      end

      def clobber
        FileUtils.rm_rf @config[:dbpath]
        self
      end
    end

  end
end
