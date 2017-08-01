# Copyright (C) 2014-2016 MongoDB, Inc.
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

module Mongo

  # A logical session representing a set of sequential operations executed
  #   by an application that are related in some way.
  #
  # @since 2.5.0
  class Session
    extend Forwardable

    # Get the client through which this session was created.
    #
    # @since 2.5.0
    attr_reader :client

    # Get the options for this session.
    #
    # @since 2.5.0
    attr_reader :options

    # Initialize a Session.
    #
    # @example
    #   Session.new(client, options)
    #
    # @param [ Mongo::Client ] client The client through which this session is created.
    # @param [ Hash ] options The options for this session.
    #
    # @since 2.5.0
    def initialize(client, options = {})
      @client = client
      @options = options
      @server_session = ServerSession.new(client)
      @operation_time = nil
      @ended = false
      ObjectSpace.define_finalizer(self, self.class.finalize(@server_session))
    end

    # Finalize the session for garbage collection. Sends an endSessions command.
    #
    # @example Finalize the session.
    #   Session.finalize(server_session)
    #
    # @param [ Mongo::Session::ServerSession ] server_session The associated server
    #   session object.
    #
    # @return [ Proc ] The Finalizer.
    #
    # @since 2.5.0
    def self.finalize(server_session)
      proc do
        begin; server_session.send(:end_sessions); rescue; end
      end
    end

    # End this session.
    #
    # @example
    #   session.end_session
    #
    # @return [ true ] Always true.
    #
    # @since 2.5.0
    def end_session
      begin; @server_session.send(:end_sessions); rescue; end
      @ended = true
    end

    # Whether this session has ended.
    #
    # @example
    #   session.ended?
    #
    # @return [ true, false ] Whether the session has ended.
    #
    # @since 2.5.0
    def ended?
      @ended
    end

    # Get a database with which this session will be associated.
    #
    # @example
    #   session.database('test')
    #
    # @param [ String ] name The database name.
    #
    # @return [ Database ] The database.
    #
    # @since 2.5.0
    def database(name)
      check_if_ended!
      Database.new(client, name, client.options.merge(options)).tap do |db|
        db.instance_variable_set(:@session, self)
      end
    end

    # Execute a block of code and cache the operation time from the result.
    #
    # @example
    #   session.use do
    #     execute_operation
    #   end
    #
    # @return [ Object ] Result of the block.
    #
    # @since 2.5.0
    def use
      check_if_ended!
      set_operation_time(yield)
    rescue Mongo::Error::OperationFailure => e
      set_operation_time(e)
      raise
    end

    # Get the read concern for this session.
    #
    # @example
    #   session.get_read_concern(collection)
    #
    # @param [ Mongo::Collection ] collection The collection whose read concern is combined
    #   with the afterClusterTime value.
    #
    # @return [ Hash ] The read concern for this session.
    #
    # @since 2.5.0
    def get_read_concern(collection, server = nil)
      if !server.standalone? && causally_consistent_reads? && @operation_time
        (collection.options[:read_concern] || {}).merge(AFTER_CLUSTER_TIME => @operation_time)
      else
        collection.options[:read_concern]
      end
    end

    # Get the read preference for this session.
    #
    # @example
    #   session.read_preference
    #
    # @return [ Hash ] The read preference for this session.
    #
    # @since 2.5.0
    def read_preference
     @read_preference ||= @options[:read] || client.read_preference
    end

    # Get the write concern for this session.
    #
    # @example
    #   session.write_concern
    #
    # @return [ Mongo::WriteConcern ] The write concern object for this session.
    #
    # @since 2.5.0
    def write_concern
      @write_concern ||= WriteConcern.get(@options[:write]|| client.write_concern)
    end

    # Get the names of all databases.
    #
    # @example Get the database names.
    #   session.database_names
    #
    # @return [ Array<String> ] The names of the databases.
    #
    # @since 2.0.5
    def database_names
      check_if_ended!
      list_databases.collect { |info| info[Database::NAME] }
    end

    # Get info for each database.
    #
    # @example Get the info for each database.
    #   client.list_databases
    #
    # @return [ Array<Hash> ] The info for each database.
    #
    # @since 2.0.5
    def list_databases
      check_if_ended!
      client.list_databases
    end

    private

    AFTER_CLUSTER_TIME = 'afterClusterTime'.freeze

    def check_if_ended!
      raise Exception if ended?
    end

    def set_operation_time(result)
      @operation_time = result.operation_time if result.operation_time
      result
    end

    def causally_consistent_reads?
      options[:causally_consistent_reads]
    end

    # An object representing the server-side session.
    #
    # @api private
    #
    # @since 2.5.0
    class ServerSession
      include Retryable

      # The command sent to the server to start a session.
      #
      # @since 2.5.0
      START_SESSION = { :startSession => 1 }.freeze

      # The command sent to the server to end a session.
      #
      # @since 2.5.0
      END_SESSION = { :endSessions => 1 }.freeze

      # The field in the startSession response from the server containing
      #   the id of the session.
      #
      # @since 2.5.0
      SESSION_ID = 'id'.freeze

      # The field in the startSession response from the server containing
      #   the timeout duration used by the server.
      #
      # @since 2.5.0
      TIMEOUT_MINUTES = 'timeoutMinutes'.freeze

      # The field in the startSession response from the server containing
      #   the last time the server session was used.
      #
      # @since 2.5.0
      LAST_USE = 'lastUse'.freeze

      # Initialize a ServerSession.
      #
      # @example
      #   ServerSession.new(client)
      #
      # @param [ Mongo::Client ] client The client that will be used to send the startSession command.
      #
      # @since 2.5.0
      def initialize(client)
        start(client)
      end

      private

      def start(client)
        server_selector = ServerSelector.get(mode: :primary_preferred)
        response = read_with_one_retry do
          server = server_selector.select_server(client.cluster)
          Operation::Commands::Command.new(:selector => START_SESSION,
                                           :db_name => :admin
                                          ).execute(server).first
        end
        @session_id = response[SESSION_ID]['signedLsid']['lsid']
        @timeout_minutes = response[TIMEOUT_MINUTES]
        @last_use = response[SESSION_ID][LAST_USE]
      end

      def end_sessions(client, ids = nil)
        read_with_one_retry do
          Operation::Commands::Command.new(:selector => END_SESSION.merge(ids: ids || [ @session_id ]),
                                           :db_name => :admin
                                          ).execute(client.cluster.next_primary)
        end
      end
    end
  end
end
