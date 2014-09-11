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

TEST_HOST     = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost' unless defined? TEST_HOST
TEST_DATA     = File.join(File.dirname(__FILE__), 'fixtures/data')
TEST_BASE     = Test::Unit::TestCase

unless defined? TEST_PORT
  TEST_PORT = if ENV['MONGO_RUBY_DRIVER_PORT']
    ENV['MONGO_RUBY_DRIVER_PORT'].to_i
  else
    Mongo::MongoClient::DEFAULT_PORT
  end
end

class Test::Unit::TestCase
  include Mongo
  include BSON

  # Handles creating a pre-defined MongoDB cluster for integration testing.
  #
  # @param  kind=nil [Symbol] Type of cluster (:rs or :sc).
  # @param  opts={} [Hash] Options to be passed through to the cluster manager.
  #
  # @return [ClusterManager] The cluster manager instance being used.
  def ensure_cluster(kind=nil, opts={})
    cluster_instance = nil
    class_vars = TEST_BASE.class_eval { class_variables }
    if class_vars.include?("@@cluster_#{kind}") || class_vars.include?("@@cluster_#{kind}".to_sym)
      cluster_instance = TEST_BASE.class_eval { class_variable_get("@@cluster_#{kind}") }
    end

    unless cluster_instance
      if kind == :rs
        cluster_opts = Config::DEFAULT_REPLICA_SET.dup
      else
        cluster_opts = Config::DEFAULT_SHARDED_SIMPLE.dup
      end

      cluster_opts.merge!(opts)
      cluster_opts.merge!(:dbpath => ENV['MONGO_DBPATH'] || 'data')
      config = Config.cluster(cluster_opts)

      cluster_instance = Config::ClusterManager.new(config)
      TEST_BASE.class_eval { class_variable_set("@@cluster_#{kind}", cluster_instance) }
    end

    cluster_instance.start
    instance_variable_set("@#{kind}", cluster_instance)

    uri = "mongodb://#{TEST_USER}:#{TEST_USER_PWD}@" +
            "#{cluster_instance.members_uri}"
    uri += "?replicaset=#{@rs.repl_set_name}" if cluster_instance.replica_set?
    instance_variable_set("@uri", uri)
  end

  # Generic helper to rescue and retry from a connection failure.
  #
  # @param max_retries=30 [Integer] The number of times to attempt a retry.
  #
  # @return [Object] The block result.
  def rescue_connection_failure(max_retries=30)
    retries = 0
    begin
      yield
    rescue Mongo::ConnectionFailure => ex
      retries += 1
      raise ex if retries > max_retries
      sleep(2)
      retry
    end
  end

  # Creates and connects a standard, pre-defined MongoClient instance.
  #
  # @param  options={} [Hash] Options to be passed to the client instance.
  # @param  legacy=false [Boolean] When true, uses deprecated Mongo::Connection.
  #
  # @return [MongoClient] The client instance.
  def self.standard_connection(options={}, legacy=false)
    if legacy
      silently do
        # We have to create the Connection object directly here instead of using TEST_URI
        # because Connection#from_uri ends up creating a MongoClient object.
        conn = Connection.new(TEST_HOST, TEST_PORT, options)
        conn[TEST_DB].authenticate(TEST_USER, TEST_USER_PWD)
        conn
      end
    else
      MongoClient.from_uri(TEST_URI, options)
    end
  end

  # Creates and connects a standard, pre-defined MongoClient instance.
  #
  # @param  options={} [Hash] Options to be passed to the client instance.
  # @param  legacy=false [Boolean] When true, uses deprecated Mongo::Connection.
  #
  # @return [MongoClient] The client instance.
  def standard_connection(options={}, legacy=false)
    self.class.standard_connection(options, legacy)
  end

  def self.host_port
    "#{mongo_host}:#{mongo_port}"
  end

  def self.mongo_host
    TEST_HOST
  end

  def self.mongo_port
    TEST_PORT
  end

  def host_port
    self.class.host_port
  end

  def mongo_host
    self.class.mongo_host
  end

  def mongo_port
    self.class.mongo_port
  end

  def method_name
    caller[0]=~/`(.*?)'/
    $1
  end

  def perform_step_down(member)
    start   = Time.now
    timeout = 20 # seconds
    begin
      step_down_command = BSON::OrderedHash.new
      step_down_command[:replSetStepDown] = 30
      member['admin'].command(step_down_command)
    rescue Mongo::OperationFailure => e
      retry unless (Time.now - start) > timeout
      raise e
    end
  end

  def new_mock_socket(host='localhost', port=27017)
    socket = Object.new
    socket.stubs(:setsockopt).with(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    socket.stubs(:close)
    socket.stubs(:closed?)
    socket.stubs(:checkin)
    socket.stubs(:pool)
    socket
  end

  def new_mock_unix_socket(sockfile='/tmp/mongod.sock')
    socket = Object.new
    socket.stubs(:setsockopt).with(Socket::IPPROTO_TCP)
    socket.stubs(:close)
    socket.stubs(:closed?)
    socket
  end

  def new_mock_db
    Object.new
  end

  def assert_raise_error(klass, message=nil)
    begin
      yield
    rescue => e
      if klass.to_s != e.class.to_s
        flunk "Expected exception class #{klass} but got #{e.class}.\n #{e.backtrace}"
      end

      if message && !e.message.include?(message)
        p e.backtrace
        flunk "#{e.message} does not include #{message}.\n#{e.backtrace}"
      end
    else
      flunk "Expected assertion #{klass} but none was raised."
    end
  end

  def match_document(key, expected, actual) # special cases for Regexp match, BSON::ObjectId, Range
    if expected.is_a?(Hash) && actual.is_a?(Hash)
      expected_keys = expected.keys.sort
      actual_keys = actual.keys.sort
      #currently allow extra fields in actual as the following check for equality of keys is commented out
      #raise "field:#{key.inspect} - Hash keys expected:#{expected_keys.inspect} actual:#{actual_keys.inspect}" if expected_keys != actual_keys
      expected_keys.each{|k| match_document(k, expected[k], actual[k])}
    elsif expected.is_a?(Array) && actual.is_a?(Array)
      raise "field:#{key.inspect} - Array size expected:#{expected.size} actual:#{actual.size}" if expected.size != actual.size
      (0...expected.size).each{|i| match_document(i, expected[i], actual[i])}
    elsif expected.is_a?(Regexp) && actual.is_a?(String)
      raise "field:#{key.inspect} - Regexp expected:#{expected.inspect} actual:#{actual.inspect}" if expected !~ actual
    elsif expected.is_a?(BSON::ObjectId) && actual.is_a?(BSON::ObjectId)
      # match type but not value
    elsif expected.is_a?(Range)
      raise "field:#{key.inspect} - Range expected:#{expected.inspect} actual:#{actual.inspect}" if !expected.include?(actual)
    elsif expected.is_a?(Set)
      raise "field:#{key.inspect} - Set expected:#{expected.inspect} actual:#{actual.inspect}" if !expected.include?(actual)
    else
      raise "field:#{key.inspect} - expected:#{expected.inspect} actual:#{actual.inspect}" if expected != actual
    end
    true
  end

  def assert_match_document(expected, actual, message = '')
    match = begin
      match_document('', expected, actual)
    rescue => ex
      message = ex.message + ' - ' + message
      false
    end
    assert(match, message)
  end

  def with_forced_timeout(client, &block)
    authenticate_client(client)
    cmd_line_args = client['admin'].command({ :getCmdLineOpts => 1 })['argv']
    if cmd_line_args.include?('enableTestCommands=1') && client.server_version >= "2.5.3"
      begin
        #Force any query or command with valid non-zero max time to fail (SERVER-10650)
        fail_point_cmd = OrderedHash.new
        fail_point_cmd[:configureFailPoint] = 'maxTimeAlwaysTimeOut'
        fail_point_cmd[:mode] = 'alwaysOn'
        client['admin'].command(fail_point_cmd)
        yield
        fail_point_cmd[:mode] = 'off'
        client['admin'].command(fail_point_cmd)
      end
    end
  end

  def with_default_journaling(client, &block)
    authenticate_client(client)
    cmd_line_args = client['admin'].command({ :getCmdLineOpts => 1 })['parsed']
    unless client.server_version < "2.0" || cmd_line_args.include?('nojournal')
      yield
    end
  end

  def with_no_replication(client, &block)
    if client.class == MongoClient
      yield
    end
  end

  def with_no_journaling(client, &block)
    authenticate_client(client)
    cmd_line_args = client['admin'].command({ :getCmdLineOpts => 1 })['parsed']
    unless client.server_version < "2.0" || !cmd_line_args.include?('nojournal')
      yield
    end
  end

  def with_ipv6_enabled(client, &block)
    authenticate_client(client)
    cmd_line_args = client['admin'].command({ :getCmdLineOpts => 1 })['parsed']
    if cmd_line_args.include?('ipv6')
      yield
    end
  end

  def with_write_commands(client, &block)
    wire_version = Mongo::MongoClient::BATCH_COMMANDS
    if client.primary_wire_version_feature?(wire_version)
      yield wire_version
    end
  end

  def with_preserved_env_uri(new_uri=nil, &block)
    old_mongodb_uri = ENV['MONGODB_URI']
    begin
      ENV['MONGODB_URI'] = new_uri
      yield
    ensure
      ENV['MONGODB_URI'] = old_mongodb_uri
    end
  end

  def with_write_operations(client, &block)
    wire_version = Mongo::MongoClient::RELEASE_2_4_AND_BEFORE
    if client.primary_wire_version_feature?(wire_version)
      client.class.class_eval(%Q{
        alias :old_use_write_command? :use_write_command?
        def use_write_command?(write_concern)
          false
        end
      })
      yield wire_version
      client.class.class_eval(%Q{
        alias :use_write_command? :old_use_write_command?
      })
    end
  end

  def with_write_commands_and_operations(client, &block)
    with_write_commands(client, &block)
    with_write_operations(client, &block)
  end

  def batch_commands?(wire_version)
    wire_version >= Mongo::MongoClient::BATCH_COMMANDS
  end

  def subject_to_server_4754?(client)
    # Until SERVER-4754 is resolved, profiling info is not collected
    # when mongod is started with --auth in versions < 2.2
    authenticate_client(client)
    cmd_line_args = client['admin'].command({ :getCmdLineOpts => 1 })['parsed']
    client.server_version < '2.2' && cmd_line_args.include?('auth')
  end

  # When testing under narrowed localhost exception, the admin user must have
  # special permissions to run the db_eval command.
  def grant_admin_user_eval_role(client)
    if auth_enabled?(client) && client.server_version >= "2.6"
      # we need to have anyAction on anyResource to run db_eval()
      admin = client['admin']
      any_priv = BSON::OrderedHash.new
      any_priv[:resource] = { :anyResource => true }
      any_priv[:actions] = ['anyAction']

      create_role = BSON::OrderedHash.new
      create_role[:createRole] = 'db_eval'
      create_role[:privileges] = [any_priv]
      create_role[:roles] = []

      begin
        admin.command(create_role)
      rescue Mongo::OperationFailure => ex
        # role already exists
      end

      grant_role = BSON::OrderedHash.new
      grant_role[:grantRolesToUser] = TEST_USER
      grant_role[:roles] = ['db_eval']
      admin.command(grant_role)
    end
  end

  # Return true if auth is enabled, false otherwise.
  def auth_enabled?(client)
    begin
      cmd_line_args = client['admin'].command({ :getCmdLineOpts => 1 })['parsed']
      return true if cmd_line_args.include?('auth') || cmd_line_args.include?('keyFile')
      if security = cmd_line_args["security"]
        return true if security["authorization"] == "enabled"
      end
    rescue OperationFailure => ex
      # under narrowed localhost exception in > 2.7.1, getCmdLineOpts is not allowed
      # unless you're authenticated.
      return true if ex.message.include?("authorized") ||
                      (client.server_version >= "2.7.1" &&
                      ex.error_code == Mongo::ErrorCode::UNAUTHORIZED)
    end
  end

  def with_auth(client, &block)
    yield if auth_enabled?(client)
  end

  def authenticate_client(client)
    client[TEST_DB].authenticate(TEST_USER, TEST_USER_PWD) unless client.auths.any? {|a| a[:source] == TEST_DB}
    client
  end

  def self.ensure_admin_user
    10.times do
      begin
        client = Mongo::MongoClient.new(TEST_HOST, TEST_PORT)
        db = client[TEST_DB]
        begin
          db.authenticate(TEST_USER, TEST_USER_PWD)
        rescue Mongo::AuthenticationError => ex
          roles = [ 'dbAdminAnyDatabase',
                    'userAdminAnyDatabase',
                    'readWriteAnyDatabase',
                    'clusterAdmin' ]
          db.add_user(TEST_USER, TEST_USER_PWD, nil, :roles => roles)
        end
        TEST_BASE.class_eval { class_variable_set("@@connected_single_mongod", true) }
        break
      rescue Mongo::ConnectionFailure
        # mongod not available yet, wait a second and try again
        sleep(1)
      end
      #puts "Not connected to a MongoD" unless client.connected?
    end
  end

  def self.cleanup_users_and_dbs
    not_cluster = TEST_BASE.class_eval { class_variables }.none? { |v| v =~ /@@cluster_/ }

    if @@connected_single_mongod && not_cluster
      client = Mongo::MongoClient.from_uri(TEST_URI)
      db = client[TEST_DB]
      begin
        begin
          db.authenticate(TEST_USER, TEST_USER_PWD)

        rescue Mongo::ConnectionFailure, Mongo::MongoArgumentError
        rescue Mongo::AuthenticationError
          Test::Unit::TestCase.ensure_admin_user
          db.authenticate(TEST_USER, TEST_USER_PWD)
        end

        client.database_names.each do |db_name|
          if db_name =~ /^ruby_test*/
            puts "[CLEAN-UP] Dropping '#{db_name}'..."
            client.drop_database(db_name)
          end
        end

        if client.server_version < '2.5'
          db['system.users'].remove
        else
          db.command(:dropAllUsersFromDatabase => 1)
        end

      rescue Mongo::ConnectionFailure
        # Nothing we can do about the mongod not being available
      end
    end
  end
end


Test::Unit.at_start do
  TEST_DB       = ENV['TEST_DB']       || 'admin'
  TEST_USER     = ENV['TEST_USER']     || 'admin_user'
  TEST_USER_PWD = ENV['TEST_USER_PWD'] || 'password'
  TEST_URI      = ENV['TEST_URI']      ||
      "mongodb://#{TEST_USER}:#{TEST_USER_PWD}@#{TEST_HOST}:#{TEST_PORT}/#{TEST_DB}"
  TEST_BASE.class_eval { class_variable_set("@@connected_single_mongod", false) }
  Test::Unit::TestCase.ensure_admin_user
end

# Before and after hooks for the entire test run
# handles mop up after the cluster manager is done.
Test::Unit.at_exit do
  Test::Unit::TestCase.cleanup_users_and_dbs
  TEST_BASE.class_eval { class_variables }.select { |v| v =~ /@@cluster_/ }.each do |cluster|
    TEST_BASE.class_eval { class_variable_get(cluster) }.stop
  end
end
