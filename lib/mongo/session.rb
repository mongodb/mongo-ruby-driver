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
    include Retryable

    # Get the options for this session.
    #
    # @since 2.5.0
    attr_reader :options

    # Get the cluster through which this session was created.
    #
    # @since 2.5.1
    attr_reader :client

    # The cluster time for this session.
    #
    # @since 2.5.0
    attr_reader :cluster_time

    # The latest seen operation time for this session.
    #
    # @since 2.5.0
    attr_reader :operation_time

    # The options for the transaction currently being executed on the session.
    #
    # @since 2.6.0
    attr_reader :txn_options

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

    # Error label describing commitTransaction errors that may or may not occur again if a commit is
    # manually retried by the user.
    #
    # @since 2.6.0
    UNKNOWN_TRANSACTION_COMMIT_LABEL = 'UnknownTransactionCommitResult'.freeze

    # Error label describing errors that will likely not occur if a transaction is manually retried
    # from the start.
    #
    # @since 2.6.0
    TRANSIENT_TRANSACTION_ERROR_LABEL = 'TransientTransactionError'.freeze

    # The state of a session in which the last operation was not related to any transaction or no
    # operations have yet occurred.
    #
    # @since 2.6.0
    NO_TRANSACTION_STATE = :no_transaction

    # The state of a session in which a user has initiated a transaction but no operations within
    # the transactions have occurred yet.
    #
    # @since 2.6.0
    STARTING_TRANSACTION_STATE = :starting_transaction

    # The state of a session in which a transaction has been started and at least one operation has
    # occurred, but the transaction has not yet been committed or aborted.
    #
    # @since 2.6.0
    TRANSACTION_IN_PROGRESS_STATE = :transaction_in_progress

    # The state of a session in which the last operation executed was a transaction commit.
    #
    # @since 2.6.0
    TRANSACTION_COMMITTED_STATE = :transaction_committed

    # The state of a session in which the last operation executed was a transaction abort.
    #
    # @since 2.6.0
    TRANSACTION_ABORTED_STATE = :transaction_aborted

    UNLABELED_WRITE_CONCERN_CODES = ['CannotSatisfyWriteConcern', 'UnknownReplWriteConcern'].freeze

    # Initialize a Session.
    #
    # @example
    #   Session.new(server_session, client, options)
    #
    # @param [ ServerSession ] server_session The server session this session is associated with.
    # @param [ Client ] client The client through which this session is created.
    # @param [ Hash ] options The options for this session.
    #
    # @since 2.5.0
    def initialize(server_session, client, options = {})
      @server_session = server_session

      # Because the read preference will need to be inserted into a command as a string, we convert
      # it from a symbol immediately upon receiving it.
      if options[:read_preference] && options[:read_preference][:mode]
        options[:read_preference][:mode] = options[:read_preference][:mode].to_s
      end

      @client = client.use(:admin)
      @options = options.dup.freeze
      @cluster_time = nil
      @state = NO_TRANSACTION_STATE
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
      if !ended? && @client
        abort_transaction if within_states?(TRANSACTION_IN_PROGRESS_STATE) rescue Mongo::Error
        @client.cluster.session_pool.checkin(@server_session)
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

    # Add the autocommit field to a command document if applicable.
    #
    # @example
    #   session.add_autocommit!(cmd)
    #
    # @return [ Hash, BSON::Document ] The command document.
    #
    # @since 2.6.0
    def add_autocommit!(command)
      command.tap do |c|
        c[:autocommit] = false if in_transaction?
      end
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

    # Add the startTransaction field to a command document if applicable.
    #
    # @example
    #   session.add_start_transaction!(cmd)
    #
    # @return [ Hash, BSON::Document ] The command document.
    #
    # @since 2.6.0
    def add_start_transaction!(command)
      command.tap do |c|
        c[:startTransaction] = true if starting_transaction?
      end
    end

    # Add the transaction number to a command document if applicable.
    #
    # @example
    #   session.add_txn_num!(cmd)
    #
    # @return [ Hash, BSON::Document ] The command document.
    #
    # @since 2.6.0
    def add_txn_num!(command)
      command.tap do |c|
        c[:txnNumber] = BSON::Int64.new(@server_session.txn_num) if in_transaction?
      end
    end

    # Add the transactions options if applicable.
    #
    # @example
    #   session.add_txn_opts!(cmd)
    #
    # @return [ Hash, BSON::Document ] The command document.
    #
    # @since 2.6.0
    def add_txn_opts!(command, read)
      command.tap do |c|
        # The read preference should be added for all read operations.
        c['$readPreference'] = txn_read_pref if read && txn_read_pref

        # The read concern should be added to any command that starts a transaction.
        if starting_transaction? && txn_read_concern
          c[:readConcern] ||= {}
          c[:readConcern].merge!(txn_read_concern)
        end

        # We need to send the read concern level as a string rather than a symbol.
        if c[:readConcern] && c[:readConcern][:level]
          c[:readConcern][:level] = c[:readConcern][:level].to_s
        end

        # The write concern should be added to any abortTransaction or commitTransaction command.
        if (c[:abortTransaction] || c[:commitTransaction]) && txn_write_concern
          c[:writeConcern] = txn_write_concern
        end

        # A non-numeric write concern w value needs to be sent as a string rather than a symbol.
        if c[:writeConcern] && c[:writeConcern][:w] && c[:writeConcern][:w].is_a?(Symbol)
          c[:writeConcern][:w] = c[:writeConcern][:w].to_s
        end
      end
    end

    # Remove the read concern and/or write concern from the command if not applicable.
    #
    # @example
    #   session.suppress_read_write_concern!(cmd)
    #
    # @return [ Hash, BSON::Document ] The command document.
    #
    # @since 2.6.0
    def suppress_read_write_concern!(command)
      command.tap do |c|
        next unless in_transaction?

        c.delete(:readConcern) unless starting_transaction?
        c.delete(:writeConcern) unless c[:commitTransaction] || c[:abortTransaction]
      end
    end

    # Ensure that the read preference of a command primary.
    #
    # @example
    #   session.validate_read_pref!(command)
    #
    # @raise [ Mongo::Error::InvalidTransactionOperation ] If the read preference of the command is
    # not primary.
    #
    # @since 2.6.0
    def validate_read_pref!(command)
      return unless in_transaction? && non_primary_readpref?(command)

      raise Mongo::Error::InvalidTransactionOperation.new(
        Mongo::Error::InvalidTransactionOperation::INVALID_READ_PREFERENCE)
    end

    # Update the state of the session due to a (non-commit and non-abort) operation being run.
    #
    # @since 2.6.0
    def update_state!
      case @state
      when STARTING_TRANSACTION_STATE
        @state = TRANSACTION_IN_PROGRESS_STATE
      when TRANSACTION_COMMITTED_STATE, TRANSACTION_ABORTED_STATE
        @state = NO_TRANSACTION_STATE
      end
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

    # Get the current transaction number.
    #
    # @example Get the current transaction number.
    #   session.txn_num
    #
    # @return [ Integer ] The current transaction number.
    #
    # @since 2.6.0
    def txn_num
      @server_session && @server_session.txn_num
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
      @implicit ||= !!(@options.key?(:implicit) && @options[:implicit] == true)
    end

    # Is this session an explicit one (i.e. user-created).
    #
    # @example Is the session explicit?
    #   session.explicit?
    #
    # @return [ true, false ] Whether this session is explicit.
    #
    # @since 2.5.2
    def explicit?
      @explicit ||= !implicit?
    end

    # Start a new transaction.
    #
    # Note that the transaction will not be started on the server until an operation is performed
    # after start_transaction is called.
    #
    # @example Start a new transaction
    #   session.start_transaction(options)
    #
    # @raise [ InvalidTransactionOperation ] If a transaction is already in progress or if the
    #   write concern is unacknowledged.
    #
    # @since 2.6.0
    def start_transaction(options = nil)
      check_if_ended!

      if within_states?(STARTING_TRANSACTION_STATE, TRANSACTION_IN_PROGRESS_STATE)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation::TRANSACTION_ALREADY_IN_PROGRESS)
      end

      next_txn_num
      @txn_options = options || @options[:default_transaction_options] || {}

      if txn_write_concern && WriteConcern.send(:unacknowledged?, txn_write_concern)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation::UNACKNOWLEDGED_WRITE_CONCERN)
      end

      @state = STARTING_TRANSACTION_STATE
    end

    # Commit the currently active transaction on the session.
    #
    # @example Commits the transaction.
    #   session.commit_transaction
    #
    # @raise [ InvalidTransactionOperation ] If a transaction was just aborted and no new one was
    #   started.
    #
    # @since 2.6.0
    def commit_transaction
      check_if_ended!
      check_if_no_transaction!

      if within_states?(TRANSACTION_ABORTED_STATE)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation.cannot_call_after(
            :abortTransaction, :commitTransaction))
      end


      begin
        # If commitTransaction is called twice, we need to run the same commit operation again, so
        # we revert the session to the previous state.
        if within_states?(TRANSACTION_COMMITTED_STATE)
          @state = @last_commit_skipped ? STARTING_TRANSACTION_STATE : TRANSACTION_IN_PROGRESS_STATE
        end

        if starting_transaction?
          @last_commit_skipped = true
        else
          @last_commit_skipped = false

          write_with_retry(self, txn_options[:write_concern], true) do |server, txn_num|
            Operation::Command.new(
              selector: { commitTransaction: 1 },
              db_name: 'admin',
              session: self,
              txn_num: txn_num
            ).execute(server)
          end
        end
      rescue Mongo::Error::NoServerAvailable, Mongo::Error::SocketError => e
        e.send(:add_label, UNKNOWN_TRANSACTION_COMMIT_LABEL)
        raise e
      rescue Mongo::Error::OperationFailure => e
        err_doc = e.instance_variable_get(:@result).send(:first_document)

        if err_doc['writeConcernError'] &&
            !UNLABELED_WRITE_CONCERN_CODES.include?(err_doc['writeConcernError']['codeName'])
          e.send(:add_label, UNKNOWN_TRANSACTION_COMMIT_LABEL)
        end

        raise e
      ensure
        @state = TRANSACTION_COMMITTED_STATE
      end
    end

    # Abort the currently active transaction without making any changes to the database.
    #
    # @example Abort the transaction.
    #   session.abort_transaction
    #
    # @raise [ Mongo::Error::InvalidTransactionOperation ] If a transaction was just committed or
    #   aborted and no new one was started.
    #
    # @since 2.6.0
    def abort_transaction
      check_if_ended!
      check_if_no_transaction!

      if within_states?(TRANSACTION_COMMITTED_STATE)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation.cannot_call_after(
            :commitTransaction, :abortTransaction))
      end

      if within_states?(TRANSACTION_ABORTED_STATE)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation.cannot_call_twice(:abortTransaction))
      end

      unless starting_transaction?
        write_with_retry(self, txn_options[:write_concern], true) do |server, txn_num|
          Operation::Command.new(
            selector: { abortTransaction: 1 },
            db_name: 'admin',
            session: self,
            txn_num: txn_num
          ).execute(server)
        end
      end

      @state = TRANSACTION_ABORTED_STATE
    rescue Mongo::Error::InvalidTransactionOperation
      raise
    rescue Mongo::Error
      @state = TRANSACTION_ABORTED_STATE
    end

    # Whether or not the session is currently in a transaction.
    #
    # @example Is the session in a transaction?
    #   session.in_transaction?
    #
    # @return [ true | false ] Whether or not the session in a transaction.
    #
    # @since 2.6.0
    def in_transaction?
      within_states?(STARTING_TRANSACTION_STATE, TRANSACTION_IN_PROGRESS_STATE)
    end

    # Get the read preference document the session will use in the currently active transaction.
    #
    # @example Get the transaction's read preference
    #   session.txn_read_pref
    #
    # @return [ Hash ] The read preference document of the transaction.
    #
    # @since 2.6.0
    def txn_read_pref
      rp = (txn_options && txn_options[:read_preference] && txn_options[:read_preference].dup) ||
        (@client.read_preference && @client.read_preference.dup)
      rp[:mode] = rp[:mode].to_s if rp
      rp
    end

    def cluster
      @client.cluster
    end

    private

    def within_states?(*states)
      states.include?(@state)
    end

    def starting_transaction?
      within_states?(STARTING_TRANSACTION_STATE)
    end

    def check_if_no_transaction!
      return unless within_states?(NO_TRANSACTION_STATE)

      raise Mongo::Error::InvalidTransactionOperation.new(
        Mongo::Error::InvalidTransactionOperation::NO_TRANSACTION_STARTED)
    end

    def txn_read_concern
      txn_options && txn_options[:read_concern] || @client.read_concern
    end

    def txn_write_concern
      (txn_options && txn_options[:write_concern]) ||
        (@client.write_concern && @client.write_concern.options)
    end

    def non_primary_readpref?(command)
      return false unless command['$readPreference']

      mode = command['$readPreference']['mode'] || command['$readPreference'][:mode]
      mode && mode != 'primary'
    end

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
      if @client.cluster != cluster
        raise Mongo::Error::InvalidSession.new(MISMATCHED_CLUSTER_ERROR_MSG)
      end
    end
  end
end
