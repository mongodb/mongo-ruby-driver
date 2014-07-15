#!/usr/bin/env ruby

# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    DEFAULT_SHARDED_SIMPLE = DEFAULT_BASE_OPTS.merge( :shards => 2, :configs => 1, :routers => 2 )
    DEFAULT_SHARDED_REPLICA = DEFAULT_SHARDED_SIMPLE.merge( :replicas => 3, :arbiters => 0)

    IGNORE_KEYS = [:host, :command, :_id]
    SHARDING_OPT_KEYS = [:shards, :configs, :routers]
    REPLICA_OPT_KEYS = [:replicas, :arbiters]
    MONGODS_OPT_KEYS = [:mongods]
    CLUSTER_OPT_KEYS = SHARDING_OPT_KEYS + REPLICA_OPT_KEYS + MONGODS_OPT_KEYS

    FLAGS = [:noprealloc, :smallfiles, :logappend, :configsvr, :shardsvr, :quiet, :fastsync, :auth, :ipv6]

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
              when :shards
                make_standalone_shard(kind, opts)
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
      ipv6       = opts[:ipv6].nil? ? true : opts[:ipv6]

      params.merge(:command    => mongod,
                   :dbpath     => path,
                   :smallfiles => smallfiles,
                   :noprealloc => noprealloc,
                   :quiet      => quiet,
                   :fastsync   => fast_sync,
                   :auth       => auth,
                   :ipv6       => ipv6)
    end

    def self.key_file(opts)
      keyFile = opts[:key_file] || '/test/fixtures/auth/keyfile'
      keyFile = Dir.pwd << keyFile
      system "chmod 600 #{keyFile}"
      keyFile
    end

    # A regular mongod minus --auth and plus --keyFile.
    def self.make_standalone_shard(kind, opts)
      params = make_mongod(kind, opts)
      params.delete(:auth)
      params.merge(:keyFile => key_file(opts))
    end

    def self.make_replica(opts, id)
      params     = make_mongod('replicas', opts)

      replSet    = opts[:replSet]    || 'ruby-driver-test'
      oplogSize  = opts[:oplog_size] || 5

      params.merge(:_id       => id,
                   :replSet   => replSet,
                   :oplogSize => oplogSize,
                   :keyFile   => key_file(opts))
    end

    def self.make_config(opts)
      params = make_mongod('configs', opts)
      params.delete(:auth)
      params.merge(:configsvr => nil,
                   :keyFile => key_file(opts))
    end

    def self.make_router(config, opts)
      params = make_mongo('routers', opts)
      mongos = ENV['MONGOS'] || 'mongos'

      params.merge(
        :command  => mongos,
        :configdb => self.configdb(config),
        :keyFile  => key_file(opts)
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
        # only for authenticate under narrowed localhost exception
        @root_user = 'admin'
        @root_pwd = 'password'

        @config = config
        @servers = {}
        Mongo::Config::CLUSTER_OPT_KEYS.each do |key|
          @servers[key] = @config[key].collect{|conf| DbServer.new(conf)} if @config[key]
        end
      end

      def servers(key = nil)
        @servers.collect{|k,v| (!key || key == k) ? v : nil}.flatten.compact
      end

      # Run the command with authentication against the admin user, if needed.
      # Note: this will only add auth for MongoDB versions >= 2.7.1.
      # For earlier versions, commands with auth run under the localhost exception.
      #
      # @param [ MongoClient ] client The client on which to authenticate, if needed.
      def ensure_auth(client, &block)
        if client.server_version < '2.7.1'
          # no narrowed localhost exception, simply yield.
          yield
        else
          if !@localhost_exception
            puts "ensuring auth against this client: #{client.inspect}"
            # if @localhost_exception is wrongly set (for example if start() is called
            # multiple times in a row before stop()) then set and yield without auth.
            begin
              client.db('admin').authenticate(@root_user, @root_pwd)
              yield
              client.db('admin').logout
            rescue Mongo::AuthenticationError => ex
              yield
            end
          else
            begin
              # attempt to run command without authenticating.
              yield
            rescue Mongo::AuthenticationError, Mongo::OperationFailure => ex
              # In case the @localhost_exception variable is falsely set, handle.
              client.db('admin').authenticate(@root_user, @root_pwd)
              yield
              client.db('admin').logout
              @localhost_exception = false
            end
          end
        end
      end

      # Run a command or commands against the given servers with the given options.
      # Return the responses from running the command against each server
      # as an array.
      #
      # @param [ Array, DbServer, Hash ] servers Servers to use. This can be an array
      #  of DbServers, an array of configurations from @config, or a single DbServer
      #  or configuration hash.
      # @param [ String ] db_name Name of database against which to run command.
      # @param [ Hash, Array ] cmd The command or commands to run.
      # @param [ Hash ] opts Options for this command.
      #
      # @return [ Array, Hash ] array of responses, or a single response as a hash.
      def command(servers, db_name, cmd, opts={})

        # if we got a singleton make it an array
        servers = servers.is_a?(Array) ? servers : [ servers ]
        cmd = cmd.is_a?(Array) ? cmd : [ cmd ]

        responses = []

        servers.each do |server|
          s = server.is_a?(DbServer) ? server.config : server
          client = Mongo::MongoClient.new(s[:host], s[:port])

          # ensure this gets authenticated if we need authentication.
          ensure_auth(client) do
            cmd.each do |c|
              response = client[db_name].command(c, opts)
              if (response["ok"] != 1 &&
                  response["code"] == Mongo::ErrorCode::UNAUTHORIZED)
                # if we have an authentcation error, ensure_auth will handle.
                raise Mongo::AuthenticationError
              elsif response["ok"] != 1 && opts.fetch(:check_response, true)
                # do we need to raise an error over other failures?
                raise Mongo::OperationFailure,
                "c#{c.inspect} opts:#{opts.inspect} failed"
              end
              responses << response
            end
          end
        end
        responses.size == 1 ? responses.first : responses
      end

      # Run the replSetGetStatus command.
      #
      # @return [ Array ] responses to the replSetGetStatus command from all members.
      def repl_set_get_status
        command(@config[:replicas],
                'admin',
                { :replSetGetStatus => 1 },
                {:check_response => false })
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
        states     = nil
        healthy    = false

        60.times do
          # enter the thunderdome...
          states  = repl_set_get_status.zip(repl_set_is_master)
          healthy = states.all? do |status, is_master|

            # check replica set status for member list
            next unless status['ok'] == 1.0 && (members = status['members'])

            # ensure all replica set members are in a valid state
            next unless members.all? { |m| [1,2,7].include?(m['state']) }

            # check for primary replica set member
            next unless (primary = members.find { |m| m['state'] == 1 })

            # check replica set member optimes
            primary_optime = primary['optime'].seconds
            next unless primary_optime && members.all? do |m|
              m['state'] == 7 || primary_optime - m['optime'].seconds < 5
            end

            # check replica set state
            case status['myState']
              when 1
                is_master['ismaster']  == true &&
                is_master['secondary'] == false
              when 2
                is_master['ismaster']  == false &&
                is_master['secondary'] == true
              when 7
                is_master['ismaster']  == false &&
                is_master['secondary'] == false
            end
          end

          return healthy if healthy
          sleep(1)
        end

        raise Mongo::OperationFailure,
          "replSet startup failed - status: #{states.inspect}"
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
        # Any status with a REMOVED node won't have the full cluster state
        status = repl_set_get_status.find {|status| status['members'].find {|m| m['state'] == 'REMOVED'}.nil?}
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
        begin
          command( @config[:routers].first, 'admin', Array(shards).collect{|s| { :addshard => "#{s[:host]}:#{s[:port]}" } } )
        rescue Mongo::OperationFailure => ex
          # Because we cannot run the listshards command under the localhost
          # exception, we run the risk of attempting to add the same shard twice.
          # Our tests may add a local db to a shard, if the cluster is still up,
          # then we can ignore this.
          raise ex unless (ex.message.include?("host already used") ||
                           ex.message.include?("local database 'ruby_test'"))
        end
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

      # Returns an array with the host and port of the current primary.
      #
      # @return [ Array ] [ host, port ]
      def primary_host_port
        members = repl_set_get_status[0]['members']
        primary = members.find { |m| m['state'] == 1 }
        return nil unless primary
        primary['name'].split(':')
      end

      # Return a connection to the primary of the replica set.
      # This is done in a way that is safe under the narrowed localhost exception.
      def primary_client
        # we need either the mongos or the primary
        if @config[:routers]
          mongos = @config[:routers].first
          Mongo::MongoClient.new(mongos[:host], mongos[:port])
        else
          name = primary_host_port
          raise Mongo::ConnectionFailure, "no primary found" unless name
          Mongo::MongoClient.new(name[0], name[1])
        end
      end

      # For narrowed localhost exception, create an admin user.
      # This method can be called multiple times.
      def enable_authentication
        client = primary_client
        if client.server_version >= '2.7.1' && @localhost_exception
          # First, attempt to login.
          begin
            client.db('admin').logout
            client.db('admin').authenticate(@root_user, @root_pwd)
          rescue Mongo::AuthenticationError => ex
            cmd = BSON::OrderedHash.new
            cmd[:createUser] = @root_user
            cmd[:pwd] = @root_pwd
            cmd[:roles] = [ 'root' ]
            client.db('admin').command(cmd)
          end
          @localhost_exception = false
        end
        client.close
      end

      # For narrowed localhost exception, remove the admin user and reset to
      # localhost exception.
      def remove_authentication
        begin
          client = primary_client
          if client.server_version >= '2.7.1' && !@localhost_exception
            client.db('admin').authenticate('admin', 'password')
            client.db('admin').command({ :dropAllRolesFromDatabase => 1 })
            client.db('admin').command({ :dropAllUsersFromDatabase => 1 })
            @localhost_exception = true
          end
          client.close
        rescue Mongo::AuthenticationError => ex
          # a test may have already removed the authentication.
          @localhost_exception = true
        rescue Mongo::ConnectionFailure => ex
          # during cleanup, the primary may have been killed before this is called.
          # Catch, and allow stop() to finish cleanup.
          @localhost_exception = true
        end
      end

      # Start up all servers and, if needed, enable authentication.
      def start
        @localhost_exception = true
        # Must start configs before mongos -- hash order not guaranteed on 1.8.X
        servers(:configs).each{|server| server.start}
        servers.each{|server| server.start}
        # TODO - sharded replica sets - pending
        if @config[:replicas]
          repl_set_initiate if repl_set_get_status.first['startupStatus'] == 3
          repl_set_startup
        end
        if @config[:routers]
          addshards
        end
        enable_authentication
        self
      end
      alias :restart :start

      # Stop all servers and, if needed, remove authentication to reset to
      # localhost exception.
      def stop
        remove_authentication
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
