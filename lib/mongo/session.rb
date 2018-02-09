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

  # A logical session representing a set of sequential operations executed
  #   by an application that are related in some way.
  #
  # @since 2.5.0
  class Session
    extend Forwardable

    # Get the options for this session.
    #
    # @since 2.5.0
    attr_reader :options

    # Get the cluster through which this session was created.
    #
    # @since 2.5.1
    attr_reader :cluster

    # The cluster time for this session.
    #
    # @since 2.5.0
    attr_reader :cluster_time

    # The latest seen operation time for this session.
    #
    # @since 2.5.0
    attr_reader :operation_time

    # Error message indicating that the session was retrieved from a client with a different cluster than that of the
    # client through which it is currently being used.
    #
    # @since 2.5.0
    MISMATCHED_CLUSTER_ERROR_MSG = 'The configuration of the client used to create this session does not match that ' +
        'of the client owning this operation. Please only use this session for operations through its parent ' +
        'client.'.freeze

    # Error message describing that the session cannot be used because it has already been ended.
    #
    # @since 2.5.0
    SESSION_ENDED_ERROR_MSG = 'This session has ended and cannot be used. Please create a new one.'.freeze

    # Error message describing that sessions are not supported by the server version.
    #
    # @since 2.5.0
    SESSIONS_NOT_SUPPORTED = 'Sessions are not supported by the connected servers.'.freeze

    # Initialize a Session.
    #
    # @example
    #   Session.new(server_session, cluster, options)
    #
    # @param [ ServerSession ] server_session The server session this session is associated with.
    # @param [ Cluster ] cluster The cluster through which this session is created.
    # @param [ Hash ] options The options for this session.
    #
    # @since 2.5.0
    def initialize(server_session, cluster, options = {})
      @server_session = server_session
      @cluster = cluster
      @options = options.dup.freeze
      @cluster_time = nil
    end

    # Get a formatted string for use in inspection.
    #
    # @example Inspect the session object.
    #   session.inspect
    #
    # @return [ String ] The session inspection.
    #
    # @since 2.5.0
    def inspect
      "#<Mongo::Session:0x#{object_id} session_id=#{session_id} options=#{@options}>"
    end

    # End this session.
    #
    # @example
    #   session.end_session
    #
    # @return [ nil ] Always nil.
    #
    # @since 2.5.0
    def end_session
      if !ended? && @cluster
        @cluster.session_pool.checkin(@server_session)
      end
    ensure
      @server_session = nil
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

    # Add this session's id to a command document.
    #
    # @example
    #   session.add_id!(cmd)
    #
    # @return [ Hash, BSON::Document ] The command document.
    #
    # @since 2.5.0
    def add_id!(command)
      command.merge!(lsid: session_id)
    end

    # Validate the session.
    #
    # @example
    #   session.validate!(cluster)
    #
    # @param [ Cluster ] cluster The cluster the session is attempted to be used with.
    #
    # @return [ nil ] nil if the session is valid.
    #
    # @raise [ Mongo::Error::InvalidSession ] Raise error if the session is not valid.
    #
    # @since 2.5.0
    def validate!(cluster)
      check_matching_cluster!(cluster)
      check_if_ended!
      self
    end

    # Process a response from the server that used this session.
    #
    # @example Process a response from the server.
    #   session.process(result)
    #
    # @param [ Operation::Result ] result The result from the operation.
    #
    # @return [ Operation::Result ] The result.
    #
    # @since 2.5.0
    def process(result)
      unless implicit?
        set_operation_time(result)
        set_cluster_time(result)
      end
      @server_session.set_last_use!
      result
    end

    # Advance the cached cluster time document for this session.
    #
    # @example Advance the cluster time.
    #   session.advance_cluster_time(doc)
    #
    # @param [ BSON::Document, Hash ] new_cluster_time The new cluster time.
    #
    # @return [ BSON::Document, Hash ] The new cluster time.
    #
    # @since 2.5.0
    def advance_cluster_time(new_cluster_time)
      if @cluster_time
        @cluster_time = [ @cluster_time, new_cluster_time ].max_by { |doc| doc[Cluster::CLUSTER_TIME] }
      else
        @cluster_time = new_cluster_time
      end
    end

    # Advance the cached operation time for this session.
    #
    # @example Advance the operation time.
    #   session.advance_operation_time(timestamp)
    #
    # @param [ BSON::Timestamp ] new_operation_time The new operation time.
    #
    # @return [ BSON::Timestamp ] The max operation time, considering the current and new times.
    #
    # @since 2.5.0
    def advance_operation_time(new_operation_time)
      if @operation_time
        @operation_time = [ @operation_time, new_operation_time ].max
      else
        @operation_time = new_operation_time
      end
    end
    
    # Will writes executed with this session be retried.
    #
    # @example Will writes be retried.
    #   session.retry_writes?
    #
    # @return [ true, false ] If writes will be retried.
    #
    # @note Retryable writes are only available on server versions at least 3.6 and with
    #   sharded clusters or replica sets.
    #
    # @since 2.5.0
    def retry_writes?
      !!cluster.options[:retry_writes] && (cluster.replica_set? || cluster.sharded?)
    end

    # Get the session id.
    #
    # @example Get the session id.
    #   session.session_id
    #
    # @return [ BSON::Document ] The session id.
    #
    # @since 2.5.0
    def session_id
      @server_session.session_id if @server_session
    end

    # Increment and return the next transaction number.
    #
    # @example Get the next transaction number.
    #   session.next_txn_num
    #
    # @return [ Integer ] The next transaction number.
    #
    # @since 2.5.0
    def next_txn_num
      @server_session.next_txn_num if @server_session
    end

    # Is this session an implicit one (not user-created).
    #
    # @example Is the session implicit?
    #   session.implicit?
    #
    # @return [ true, false ] Whether this session is implicit.
    #
    # @since 2.5.1
    def implicit?
      @implicit_session ||= !!(@options.key?(:implicit) && @options[:implicit] == true)
    end

    private

    def causal_consistency_doc(read_concern)
      if operation_time && causal_consistency?
        (read_concern || {}).merge(:afterClusterTime => operation_time)
      else
        read_concern
      end
    end

    def causal_consistency?
      @causal_consistency ||= (if @options.key?(:causal_consistency)
                                 @options[:causal_consistency] == true
                               else
                                 true
                               end)
    end

    def set_operation_time(result)
      if result && result.operation_time
        @operation_time = result.operation_time
      end
    end

    def set_cluster_time(result)
      if cluster_time_doc = result.cluster_time
        if @cluster_time.nil?
          @cluster_time = cluster_time_doc
        elsif cluster_time_doc[Cluster::CLUSTER_TIME] > @cluster_time[Cluster::CLUSTER_TIME]
          @cluster_time = cluster_time_doc
        end
      end
    end

    def check_if_ended!
      raise Mongo::Error::InvalidSession.new(SESSION_ENDED_ERROR_MSG) if ended?
    end

    def check_matching_cluster!(cluster)
      if @cluster != cluster
        raise Mongo::Error::InvalidSession.new(MISMATCHED_CLUSTER_ERROR_MSG)
      end
    end
  end
end
