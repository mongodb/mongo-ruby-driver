# Copyright (C) 2017 MongoDB, Inc.
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

    def_delegators :@server_session, :session_id

    # Get the client with which this session was created.
    #
    # @since 2.5.0
    attr_reader :client

    # Get the options for this session.
    #
    # @since 2.5.0
    attr_reader :options

    # Get last seen operation time.
    #
    # @since 2.5.0
    attr_reader :operation_time

    # Error message describing that the session was attempted to be used by a cluster different from the
    # one it was originally associated with.
    #
    # @since 2.5.0
    MISTMATCHED_CLUSTER_ERROR_MSG = 'The cluster used to create this session does not match that of client ' +
        'initiating this operation. Please only use this session for operations on its original client.'.freeze

    # Error message describing that the session cannot be used because it has already been ended.
    #
    # @since 2.5.0
    SESSION_ENDED_ERROR_MSG = 'This session has ended. Please create a new one.'.freeze

    # Error message describing that sessions are not supported by the server version.
    #
    # @since 2.5.0
    SESSIONS_NOT_SUPPORTED = 'Sessions are not supported by the server version.'.freeze

    # Initialize a Session.
    #
    # @example
    #   Session.new(client, options)
    #
    # @param [ Client ] client The client through which this session is created.
    # @param [ Hash ] options The options for this session.
    #
    # @since 2.5.0
    def initialize(client, options = {})
      @client = client
      @options = options
      @server_session = ServerSession.new(client)
      @operation_time = options[:operation_time]
      @cluster_time = options[:cluster_time]
      @ended = false
      @last_use = Time.now
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
      begin; @server_session.send(:end_sessions, client); rescue; end
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

    # Execute an operation in the context of this session.
    #
    # @example
    #   session.execute do
    #     ...
    #   end
    #
    # @return [ Object ] Result of the block.
    #
    # @since 2.5.0
    def execute
      process(yield)
    rescue Mongo::Error::OperationFailure => e
      process(e)
      raise
    end

    # Add this session's id to a command document.
    #
    # @example
    #   session.add_id(cmd)
    #
    # @return [ Hash, BSON::Document ] The command document.
    #
    # @since 2.5.0
    def add_id(command)
      command.merge(lsid: session_id)
    end

    # Get the read concern for this session.
    #
    # @example
    #   session.read_concern(doc, server)
    #
    # @param [ BSON::Document, Hash ] doc The command document to which the
    #   read concern should be added.
    # @param [ Mongo::Server ] server The server to which the command is being sent.
    #
    # @return [ Hash ] The read concern for this session.
    #
    # @since 2.5.0
    def read_concern(doc, server)
      return doc unless causally_consistent_reads? && server && !server.standalone?
      causally_consistent_read_concern(doc)
    end

    # Validate the session.
    #
    # @example
    #   session.validate(client)
    #
    # @param [ Client ] client The client the session is associated with.
    #
    # @return [ nil ] Return nil if the session is valid.
    #
    # @raise [ Mongo::Error::InvalidSession ] Raise error if the session is not valid.
    #
    # @since 2.5.0
    def validate!(client)
      check_matching_client!(client)
      check_if_ended!
    end

    private

    def process(result)
      set_operation_time(result)
      set_last_use!
      result
    end

    def check_if_ended!
      raise Mongo::Error::InvalidSession.new(SESSION_ENDED_ERROR_MSG) if ended?
    end

    def check_matching_client!(client)
      if @client.cluster != client.cluster
        raise Mongo::Error::InvalidSession.new(MISTMATCHED_CLUSTER_ERROR_MSG)
      end
    end

    def set_operation_time(result)
      if result && result.operation_time
        @operation_time = result.operation_time
        result
      end
    end

    def causally_consistent_reads?
      options[:causally_consistent_reads]
    end

    def causally_consistent_read_concern(doc)
      return doc unless @operation_time
      (doc || {}).merge(AFTER_CLUSTER_TIME => @operation_time)
    end

    def set_last_use!
      @last_use = Time.now
    end

    class << self

      # Get a session or use a session to execute a block.
      #
      # @example Get a session.
      #   Session.with_session(client)
      #
      # @example Use a session.
      #   Session.with_session(client) do |session|
      #     ...
      #   end
      #
      # @param [ Client ] The client to associate the session with.
      # @param [ Hash ] options The options for the session.
      #
      # @option options [ Session ] :session The session to use or return.
      #
      # @since 2.5.0
      def with_session(client, options = {})
        # @todo: check if not a standalone
        if session = options[:session]
          session.validate!(client)
          return session unless block_given?
          session.execute do
            yield(session)
          end
        elsif sessions_supported?(client)
          session = new(client, options)
          return session unless block_given?
          result = session.execute do
            yield(session)
          end
          session.end_session
          result
        else
          yield if block_given?
        end
      end

      # Are sessions supported?
      #
      # @example Session.sessions_supported?(client)
      #
      # @return [ true, false ] Whether sessions are supported.
      #
      # @since 2.5.0
      def sessions_supported?(client)
        server = client.cluster.servers.find.first
        server && server.features.sessions_enabled? && client.cluster.logical_session_timeout
      end
    end

    # An object representing the server-side session.
    #
    # @api private
    #
    # @since 2.5.0
    class ServerSession
      include Retryable

      attr_reader :session_id

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
        # response = read_with_one_retry do
        #   client.use(:admin).command(START_SESSION, read: { mode: :primary_preferred })
        # end
        @session_id = { id: BSON::Binary.new("p4\x8F]\xB8\xCDI*\xA1q2A\x91\xC0\xABd", :uuid) }
        @timeout_minutes = client.cluster.logical_session_timeout
      end

      def end_sessions(client, ids = nil)
        read_with_one_retry do
          #client.use(:admin).command(END_SESSION.merge(ids: ids || [ @session_id ]))
        end
      end
    end
  end
end