# Copyright (C) 2017-2019 MongoDB, Inc.
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
  # by an application that are related in some way.
  #
  # @note Session objects are not thread-safe. An application may use a session
  #   from only one thread or process at a time.
  #
  # @since 2.5.0
  class Session
    extend Forwardable
    include Retryable
    include Loggable
    include ClusterTime::Consumer

    # Get the options for this session.
    #
    # @since 2.5.0
    attr_reader :options

    # Get the client through which this session was created.
    #
    # @since 2.5.1
    attr_reader :client

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

    # The state of a session in which the last operation was not related to
    # any transaction or no operations have yet occurred.
    #
    # @since 2.6.0
    NO_TRANSACTION_STATE = :no_transaction

    # The state of a session in which a user has initiated a transaction but
    # no operations within the transactions have occurred yet.
    #
    # @since 2.6.0
    STARTING_TRANSACTION_STATE = :starting_transaction

    # The state of a session in which a transaction has been started and at
    # least one operation has occurred, but the transaction has not yet been
    # committed or aborted.
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

    UNLABELED_WRITE_CONCERN_CODES = [
      79,  # UnknownReplWriteConcern
      100, # CannotSatisfyWriteConcern,
    ].freeze

    # Initialize a Session.
    #
    # @note Applications should use Client#start_session to begin a session.
    #
    # @example
    #   Session.new(server_session, client, options)
    #
    # @param [ ServerSession ] server_session The server session this session is associated with.
    # @param [ Client ] client The client through which this session is created.
    # @param [ Hash ] options The options for this session.
    #
    # @option options [ true|false ] :causal_consistency Whether to enable
    #   causal consistency for this session.
    # @option options [ Hash ] :default_transaction_options Options to pass
    #   to start_transaction by default, can contain any of the options that
    #   start_transaction accepts.
    # @option options [ true|false ] :implicit For internal driver use only -
    #   specifies whether the session is implicit.
    # @option options [ Hash ] :read_preference The read preference options hash,
    #   with the following optional keys:
    #   - *:mode* -- the read preference as a string or symbol; valid values are
    #     *:primary*, *:primary_preferred*, *:secondary*, *:secondary_preferred*
    #     and *:nearest*.
    #
    # @since 2.5.0
    # @api private
    def initialize(server_session, client, options = {})
      @server_session = server_session
      options = options.dup

      # Because the read preference will need to be inserted into a command as a string, we convert
      # it from a symbol immediately upon receiving it.
      if options[:read_preference] && options[:read_preference][:mode]
        options[:read_preference][:mode] = options[:read_preference][:mode].to_s
      end

      @client = client.use(:admin)
      @options = options.freeze
      @cluster_time = nil
      @state = NO_TRANSACTION_STATE
    end

    # @return [ Server | nil ] The server (which should be a mongos) that this
    #   session is pinned to, if any.
    #
    # @api private
    attr_reader :pinned_server

    # @return [ BSON::Document | nil ] Recovery token for the sharded
    #   transaction being executed on this session, if any.
    #
    # @api private
    attr_accessor :recovery_token

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

    # Pins this session to the specified server, which should be a mongos.
    #
    # @param [ Server ] server The server to pin this session to.
    #
    # @api private
    def pin(server)
      if server.nil?
        raise ArgumentError, 'Cannot pin to a nil server'
      end
      if Lint.enabled?
        unless server.mongos?
          raise Error::LintError, "Attempted to pin the session to server #{server.summary} which is not a mongos"
        end
      end
      @pinned_server = server
    end

    # Unpins this session from the pinned server, if the session was pinned.
    #
    # @api private
    def unpin
      @pinned_server = nil
    end

    # Unpins this session from the pinned server, if the session was pinned
    # and the specified exception instance and the session's transaction state
    # require it to be unpinned.
    #
    # The exception instance should already have all of the labels set on it
    # (both client- and server-side generated ones).
    #
    # @param [ Error ] The exception instance to process.
    #
    # @api private
    def unpin_maybe(error)
      if !within_states?(Session::NO_TRANSACTION_STATE) &&
        error.label?('TransientTransactionError')
      then
        unpin
      end

      if committing_transaction? &&
        error.label?('UnknownTransactionCommitResult')
      then
        unpin
      end
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
        if within_states?(TRANSACTION_IN_PROGRESS_STATE)
          begin
            abort_transaction
          rescue Mongo::Error
          end
        end
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
    # @api private
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
    # @api private
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
    # @api private
    def add_start_transaction!(command)
      command.tap do |c|
        if starting_transaction?
          c[:startTransaction] = true
        end
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
    # @api private
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
    # @api private
    def add_txn_opts!(command, read)
      command.tap do |c|
        # The read preference should be added for all read operations.
        if read && txn_read_pref = txn_read_preference
          Mongo::Lint.validate_underscore_read_preference(txn_read_pref)
          txn_read_pref = txn_read_pref.dup
          txn_read_pref[:mode] = txn_read_pref[:mode].to_s.gsub(/(_\w)/) { |match| match[1].upcase }
          Mongo::Lint.validate_camel_case_read_preference(txn_read_pref)
          c['$readPreference'] = txn_read_pref
        end

        # The read concern should be added to any command that starts a transaction.
        if starting_transaction?
          # https://jira.mongodb.org/browse/SPEC-1161: transaction's
          # read concern overrides collection/database/client read concerns,
          # even if transaction's read concern is not set.
          # Read concern here is the one sent to the server and may
          # include afterClusterTime.
          if rc = c[:readConcern]
            rc = rc.dup
            rc.delete(:level)
          end
          if txn_read_concern
            if rc
              rc.update(txn_read_concern)
            else
              rc = txn_read_concern.dup
            end
          end
          if rc.nil? || rc.empty?
            c.delete(:readConcern)
          else
            c[:readConcern ] = rc
          end
        end

        # We need to send the read concern level as a string rather than a symbol.
        if c[:readConcern] && c[:readConcern][:level]
          c[:readConcern][:level] = c[:readConcern][:level].to_s
        end

        # The write concern should be added to any abortTransaction or commitTransaction command.
        if (c[:abortTransaction] || c[:commitTransaction])
          if @already_committed
            wc = BSON::Document.new(c[:writeConcern] || txn_write_concern || {})
            wc.merge!(w: :majority)
            wc[:wtimeout] ||= 10000
            c[:writeConcern] = wc
          elsif txn_write_concern
            c[:writeConcern] ||= txn_write_concern
          end
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
    # @api private
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
    #   session.validate_read_preference!(command)
    #
    # @raise [ Mongo::Error::InvalidTransactionOperation ] If the read preference of the command is
    #   not primary.
    #
    # @since 2.6.0
    # @api private
    def validate_read_preference!(command)
      return unless in_transaction? && non_primary_read_preference_mode?(command)

      raise Mongo::Error::InvalidTransactionOperation.new(
        Mongo::Error::InvalidTransactionOperation::INVALID_READ_PREFERENCE)
    end

    # Update the state of the session due to a (non-commit and non-abort) operation being run.
    #
    # @since 2.6.0
    # @api private
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
    # @api private
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
    # @api private
    def process(result)
      unless implicit?
        set_operation_time(result)
        if cluster_time_doc = result.cluster_time
          advance_cluster_time(cluster_time_doc)
        end
      end
      @server_session.set_last_use!

      if doc = result.reply && result.reply.documents.first
        if doc[:recoveryToken]
          self.recovery_token = doc[:recoveryToken]
        end
      end

      result
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

    # Whether reads executed with this session can be retried according to
    # the modern retryable reads specification.
    #
    # If this method returns true, the modern retryable reads have been
    # requested by the application. If the server selected for a read operation
    # supports modern retryable reads, they will be used for that particular
    # operation. If the server selected for a read operation does not support
    # modern retryable reads, the read will not be retried.
    #
    # If this method returns false, legacy retryable reads have been requested
    # by the application. Legacy retryable read logic will be used regardless
    # of server version of the server(s) that the client is connected to.
    # The number of read retries is given by :max_read_retries client option,
    # which is 1 by default and can be set to 0 to disable legacy read retries.
    #
    # @api private
    def retry_reads?
      client.options[:retry_reads] != false
    end

    # Will writes executed with this session be retried.
    #
    # @example Will writes be retried.
    #   session.retry_writes?
    #
    # @return [ true, false ] If writes will be retried.
    #
    # @note Retryable writes are only available on server versions at least 3.6
    #   and with sharded clusters or replica sets.
    #
    # @since 2.5.0
    def retry_writes?
      !!client.options[:retry_writes] && (cluster.replica_set? || cluster.sharded?)
    end

    # Get the server session id of this session, if the session was not ended.
    # If the session was ended, returns nil.
    #
    # @example Get the session id.
    #   session.session_id
    #
    # @return [ BSON::Document ] The server session id.
    #
    # @since 2.5.0
    def session_id
      if ended?
        raise Error::SessionEnded
      end

      @server_session.session_id
    end

    # Increment and return the next transaction number.
    #
    # @example Get the next transaction number.
    #   session.next_txn_num
    #
    # @return [ Integer ] The next transaction number.
    #
    # @since 2.5.0
    # @api private
    def next_txn_num
      if ended?
        raise Error::SessionEnded
      end

      @server_session.next_txn_num
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
      if ended?
        raise Error::SessionEnded
      end

      @server_session.txn_num
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

    # Places subsequent operations in this session into a new transaction.
    #
    # Note that the transaction will not be started on the server until an
    # operation is performed after start_transaction is called.
    #
    # @example Start a new transaction
    #   session.start_transaction(options)
    #
    # @param [ Hash ] options The options for the transaction being started.
    #
    # @option options [ Hash ] read_concern The read concern options hash,
    #   with the following optional keys:
    #   - *:level* -- the read preference level as a symbol; valid values
    #      are *:local*, *:majority*, and *:snapshot*
    # @option options [ Hash ] :write_concern The write concern options. Can be :w =>
    #   Integer|String, :fsync => Boolean, :j => Boolean.
    # @option options [ Hash ] :read The read preference options. The hash may have the following
    #   items:
    #   - *:mode* -- read preference specified as a symbol; the only valid value is
    #     *:primary*.
    #
    # @raise [ Error::InvalidTransactionOperation ] If a transaction is already in
    #   progress or if the write concern is unacknowledged.
    #
    # @since 2.6.0
    def start_transaction(options = nil)
      if options
        Lint.validate_read_concern_option(options[:read_concern])
      end

      check_if_ended!

      if within_states?(STARTING_TRANSACTION_STATE, TRANSACTION_IN_PROGRESS_STATE)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation::TRANSACTION_ALREADY_IN_PROGRESS)
      end

      unpin

      next_txn_num
      @txn_options = options || @options[:default_transaction_options] || {}

      if txn_write_concern && WriteConcern.send(:unacknowledged?, txn_write_concern)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation::UNACKNOWLEDGED_WRITE_CONCERN)
      end

      @state = STARTING_TRANSACTION_STATE
      @already_committed = false
    end

    # Commit the currently active transaction on the session.
    #
    # @example Commits the transaction.
    #   session.commit_transaction
    #
    # @option options :write_concern [ nil | WriteConcern::Base ] The write
    #   concern to use for this operation.
    #
    # @raise [ Error::InvalidTransactionOperation ] If there is no active transaction.
    #
    # @since 2.6.0
    def commit_transaction(options=nil)
      check_if_ended!
      check_if_no_transaction!

      if within_states?(TRANSACTION_ABORTED_STATE)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation.cannot_call_after_msg(
            :abortTransaction, :commitTransaction))
      end

      options ||= {}

      begin
        # If commitTransaction is called twice, we need to run the same commit
        # operation again, so we revert the session to the previous state.
        if within_states?(TRANSACTION_COMMITTED_STATE)
          @state = @last_commit_skipped ? STARTING_TRANSACTION_STATE : TRANSACTION_IN_PROGRESS_STATE
          @already_committed = true
        end

        if starting_transaction?
          @last_commit_skipped = true
        else
          @last_commit_skipped = false
          @committing_transaction = true

          write_concern = options[:write_concern] || txn_options[:write_concern]
          if write_concern && !write_concern.is_a?(WriteConcern::Base)
            write_concern = WriteConcern.get(write_concern)
          end
          write_with_retry(self, write_concern, true) do |server, txn_num, is_retry|
            if is_retry
              if write_concern
                wco = write_concern.options.merge(w: :majority)
                wco[:wtimeout] ||= 10000
                write_concern = WriteConcern.get(wco)
              else
                write_concern = WriteConcern.get(w: :majority, wtimeout: 10000)
              end
            end
            Operation::Command.new(
              selector: { commitTransaction: 1 },
              db_name: 'admin',
              session: self,
              txn_num: txn_num,
              write_concern: write_concern,
            ).execute(server)
          end
        end
      ensure
        @state = TRANSACTION_COMMITTED_STATE
        @committing_transaction = false
      end
    end

    # Abort the currently active transaction without making any changes to the database.
    #
    # @example Abort the transaction.
    #   session.abort_transaction
    #
    # @raise [ Error::InvalidTransactionOperation ] If there is no active transaction.
    #
    # @since 2.6.0
    def abort_transaction
      check_if_ended!
      check_if_no_transaction!

      if within_states?(TRANSACTION_COMMITTED_STATE)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation.cannot_call_after_msg(
            :commitTransaction, :abortTransaction))
      end

      if within_states?(TRANSACTION_ABORTED_STATE)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation.cannot_call_twice_msg(:abortTransaction))
      end

      begin
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
      rescue Exception
        @state = TRANSACTION_ABORTED_STATE
        raise
      end
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

    # @return [ true | false ] Whether the session is currently committing a
    #   transaction.
    #
    # @api private
    def committing_transaction?
      !!@committing_transaction
    end

    # Executes the provided block in a transaction, retrying as necessary.
    #
    # Returns the return value of the block.
    #
    # Exact number of retries and when they are performed are implementation
    # details of the driver; the provided block should be idempotent, and
    # should be prepared to be called more than once. The driver may retry
    # the commit command within an active transaction or it may repeat the
    # transaction and invoke the block again, depending on the error
    # encountered if any. Note also that the retries may be executed against
    # different servers.
    #
    # Transactions cannot be nested - InvalidTransactionOperation will be raised
    # if this method is called when the session already has an active transaction.
    #
    # Exceptions raised by the block which are not derived from Mongo::Error
    # stop processing, abort the transaction and are propagated out of
    # with_transaction. Exceptions derived from Mongo::Error may be
    # handled by with_transaction, resulting in retries of the process.
    #
    # Currently, with_transaction will retry commits and block invocations
    # until at least 120 seconds have passed since with_transaction started
    # executing. This timeout is not configurable and may change in a future
    # driver version.
    #
    # @note with_transaction contains a loop, therefore the if with_transaction
    #   itself is placed in a loop, its block should not call next or break to
    #   control the outer loop because this will instead affect the loop in
    #   with_transaction. The driver will warn and abort the transaction
    #   if it detects this situation.
    #
    # @example Execute a statement in a transaction
    #   session.with_transaction(write_concern: {w: :majority}) do
    #     collection.update_one({ id: 3 }, { '$set' => { status: 'Inactive'} },
    #                           session: session)
    #
    #   end
    #
    # @example Execute a statement in a transaction, limiting total time consumed
    #   Timeout.timeout(5) do
    #     session.with_transaction(write_concern: {w: :majority}) do
    #       collection.update_one({ id: 3 }, { '$set' => { status: 'Inactive'} },
    #                             session: session)
    #
    #     end
    #   end
    #
    # @param [ Hash ] options The options for the transaction being started.
    #   These are the same options that start_transaction accepts.
    #
    # @raise [ Error::InvalidTransactionOperation ] If a transaction is already in
    #   progress or if the write concern is unacknowledged.
    #
    # @since 2.7.0
    def with_transaction(options=nil)
      # Non-configurable 120 second timeout for the entire operation
      deadline = Time.now + 120
      transaction_in_progress = false
      loop do
        commit_options = {}
        if options
          commit_options[:write_concern] = options[:write_concern]
        end
        start_transaction(options)
        transaction_in_progress = true
        begin
          rv = yield self
        rescue Exception => e
          if within_states?(STARTING_TRANSACTION_STATE, TRANSACTION_IN_PROGRESS_STATE)
            abort_transaction
            transaction_in_progress = false
          end

          if Time.now >= deadline
            transaction_in_progress = false
            raise
          end

          if e.is_a?(Mongo::Error) && e.label?('TransientTransactionError')
            next
          end

          raise
        else
          if within_states?(TRANSACTION_ABORTED_STATE, NO_TRANSACTION_STATE, TRANSACTION_COMMITTED_STATE)
            transaction_in_progress = false
            return rv
          end

          begin
            commit_transaction(commit_options)
            transaction_in_progress = false
            return rv
          rescue Mongo::Error => e
            if e.label?('UnknownTransactionCommitResult')
              # WriteConcernFailed
              if e.is_a?(Mongo::Error::OperationFailure) && e.code == 64 && e.wtimeout?
                transaction_in_progress = false
                raise
              end
              if Time.now >= deadline
                transaction_in_progress = false
                raise
              end
              wc_options = case v = commit_options[:write_concern]
                when WriteConcern::Base
                  v.options
                when nil
                  {}
                else
                  v
                end
              commit_options[:write_concern] = wc_options.merge(w: :majority)
              retry
            elsif e.label?('TransientTransactionError')
              if Time.now >= deadline
                transaction_in_progress = false
                raise
              end
              next
            else
              transaction_in_progress = false
              raise
            end
          end
        end
      end
    ensure
      if transaction_in_progress
        log_warn('with_transaction callback altered with_transaction loop, aborting transaction')
        begin
          abort_transaction
        rescue Error::OperationFailure, Error::InvalidTransactionOperation
        end
      end
    end

    # Get the read preference the session will use in the currently
    # active transaction.
    #
    # This is a driver style hash with underscore keys.
    #
    # @example Get the transaction's read preference
    #   session.txn_read_preference
    #
    # @return [ Hash ] The read preference of the transaction.
    #
    # @since 2.6.0
    def txn_read_preference
      rp = txn_options && txn_options[:read_preference] ||
        @client.read_preference
      Mongo::Lint.validate_underscore_read_preference(rp)
      rp
    end

    def cluster
      @client.cluster
    end

    # @api private
    def starting_transaction?
      within_states?(STARTING_TRANSACTION_STATE)
    end

    private

    # Get the read concern the session will use when starting a transaction.
    #
    # This is a driver style hash with underscore keys.
    #
    # @example Get the session's transaction read concern.
    #   session.txn_read_concern
    #
    # @return [ Hash ] The read concern used for starting transactions.
    #
    # @since 2.9.0
    def txn_read_concern
      # Read concern is inherited from client but not db or collection.
      txn_options && txn_options[:read_concern] || @client.read_concern
    end

    def within_states?(*states)
      states.include?(@state)
    end

    def check_if_no_transaction!
      return unless within_states?(NO_TRANSACTION_STATE)

      raise Mongo::Error::InvalidTransactionOperation.new(
        Mongo::Error::InvalidTransactionOperation::NO_TRANSACTION_STARTED)
    end

    def txn_write_concern
      (txn_options && txn_options[:write_concern]) ||
        (@client.write_concern && @client.write_concern.options)
    end

    def non_primary_read_preference_mode?(command)
      return false unless command['$readPreference']

      mode = command['$readPreference']['mode'] || command['$readPreference'][:mode]
      mode && mode != 'primary'
    end

    # Returns causal consistency document if the last operation time is
    # known and causal consistency is enabled, otherwise returns nil.
    def causal_consistency_doc
      if operation_time && causal_consistency?
        {:afterClusterTime => operation_time}
      else
        nil
      end
    end

    def causal_consistency?
      @causal_consistency ||= (if @options.key?(:causal_consistency)
                                 !!@options[:causal_consistency]
                               else
                                 true
                               end)
    end

    def set_operation_time(result)
      if result && result.operation_time
        @operation_time = result.operation_time
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
