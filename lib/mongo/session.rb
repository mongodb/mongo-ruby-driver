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

require 'mongo/session/session_pool'
require 'mongo/session/server_session'

module Mongo

    # A logical client session representing a set of sequential operations executed
  #   by an application that are related in some way.
  #
  # @since 2.5.0
  class Session
    extend Forwardable

    # Get the options for this session.
    #
    # @since 2.5.0
    attr_reader :options

    # Get the last-seen operation time by this session.
    #
    # @since 2.5.0
    attr_reader :operation_time

    # Get the corresponding server session for this client session.
    #
    # @since 2.5.0
    attr_reader :server_session

    # Error message describing that the session was attempted to be used by a client different from the
    # one it was originally associated with.
    #
    # @since 2.5.0
    MISTMATCHED_CLUSTER_ERROR_MSG = 'The client used to create this session does not match that of client ' +
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
    # @param [ ServerSession ] server_session The server session this client session is associated with.
    # @param [ Client ] client The client through which this session is created.
    # @param [ Hash ] options The options for this session.
    #
    # @since 2.5.0
    def initialize(server_session, client = nil, options = {})
      @server_session = server_session
      @initial_client = client
      @options = options
    end

    # End this session.
    #
    # @example
    #   session.end_session
    #
    # @return [ nil ] Always nil.
    #
    # @since 2.5.0
    def end_session(client)
      begin; client.send(:checkin_session, @server_session); rescue; end
    ensure
      @server_session = nil
    end

    def end_temp_session(client)
      unless user_created?
        end_session(client)
      end
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
      @server_session.nil?
    end

    # Execute an operation in the context of this session.
    #
    # @example
    #   session.use do
    #     ...
    #   end
    #
    # @return [ Object ] Result of the block.
    #
    # @since 2.5.0
    def use
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
      command.merge(lsid: @server_session.session_id)
    end

    # Validate the session.
    #
    # @example
    #   session.validate!(client)
    #
    # @param [ Client ] client The client the session is attempted to be used with.
    #
    # @return [ nil ] nil if the session is valid.
    #
    # @raise [ Mongo::Error::InvalidSession ] Raise error if the session is not valid.
    #
    # @since 2.5.0
    def validate!(client)
      check_matching_client!(client)
      check_if_ended!
    end

    private

    def user_created?
      !!@initial_client
    end

    def process(result)
      set_operation_time(result)
      @server_session.set_last_use!
      result
    end

    def set_operation_time(result)
      if result && result.operation_time
        @operation_time = result.operation_time
        result
      end
    end

    def check_if_ended!
      raise Mongo::Error::InvalidSession.new(SESSION_ENDED_ERROR_MSG) if ended?
    end

    def check_matching_client!(client)
      if user_created? && @initial_client != client
        raise Mongo::Error::InvalidSession.new(MISTMATCHED_CLUSTER_ERROR_MSG)
      end
    end
  end
end