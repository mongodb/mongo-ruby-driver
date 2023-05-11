# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2017-2020 MongoDB Inc.
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

    # Initialize a Session.
    #
    # A session can be explicit or implicit. Lifetime of explicit sessions is
    # managed by the application - applications explicitry create such sessions
    # and explicitly end them. Implicit sessions are created automatically by
    # the driver when sending operations to servers that support sessions
    # (3.6+), and their lifetime is managed by the driver.
    #
    # When an implicit session is created, it cannot have a server session
    # associated with it. The server session will be checked out of the
    # session pool when an operation using this session is actually executed.
    # When an explicit session is created, it must reference a server session
    # that is already allocated.
    #
    # @note Applications should use Client#start_session to begin a session.
    #   This constructor is for internal driver use only.
    #
    # @param [ ServerSession | nil ] server_session The server session this session is associated with.
    #   If the :implicit option is true, this must be nil.
    # @param [ Client ] client The client through which this session is created.
    # @param [ Hash ] options The options for this session.
    #
    # @option options [ true|false ] :causal_consistency Whether to enable
    #   causal consistency for this session.
    # @option options [ Hash ] :default_transaction_options Options to pass
    #   to start_transaction by default, can contain any of the options that
    #   start_transaction accepts.
    # @option options [ true|false ] :implicit For internal driver use only -
    #   specifies whether the session is implicit. If this is true, the server_session
    #   will be nil. This is done so that the server session is only checked
    #   out after the connection is checked out.
    # @option options [ Hash ] :read_preference The read preference options hash,
    #   with the following optional keys:
    #   - *:mode* -- the read preference as a string or symbol; valid values are
    #     *:primary*, *:primary_preferred*, *:secondary*, *:secondary_preferred*
    #     and *:nearest*.
    # @option options [ true | false ] :snapshot Set up the session for
    #   snapshot reads.
    #
    # @since 2.5.0
    # @api private
    def initialize(server_session, client, options = {})
      if options[:causal_consistency] && options[:snapshot]
        raise ArgumentError, ':causal_consistency and :snapshot options cannot be both set on a session'
      end

      if options[:implicit]
        unless server_session.nil?
          raise ArgumentError, 'Implicit session cannot reference server session during construction'
        end
      else
        if server_session.nil?
          raise ArgumentError, 'Explicit session must reference server session during construction'
        end
      end

      @server_session = server_session
      options = options.dup

      @client = client.use(:admin)
      @options = options.dup.freeze
      @cluster_time = nil
      @state = NO_TRANSACTION_STATE
    end

    # @return [ Hash ] The options for this session.
    #
    # @since 2.5.0
    attr_reader :options

    # @return [ Client ] The client through which this session was created.
    #
    # @since 2.5.1
    attr_reader :client

    def cluster
      @client.cluster
    end

    # @return [ true | false ] Whether the session is configured for snapshot
    #   reads.
    def snapshot?
      !!options[:snapshot]
    end

    # @return [ BSON::Timestamp ] The latest seen operation time for this session.
    #
    # @since 2.5.0
    attr_reader :operation_time

    # @return [ Hash ] The options for the transaction currently being executed
    # on this session.
    #
    # @since 2.6.0
    def txn_options
      @txn_options or raise ArgumentError, "There is no active transaction"
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
      !implicit?
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
    #   and with sharded clusters, replica sets, or load-balanced topologies.
    #
    # @since 2.5.0
    def retry_writes?
      !!client.options[:retry_writes] && (cluster.replica_set? || cluster.sharded? || cluster.load_balanced?)
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
      rp = txn_options[:read] ||
        @client.read_preference
      Mongo::Lint.validate_underscore_read_preference(rp)
      rp
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
      !!@ended
    end

    # Get the server session id of this session, if the session has not been
    # ended. If the session had been ended, raises Error::SessionEnded.
    #
    # @return [ BSON::Document ] The server session id.
    #
    # @raise [ Error::SessionEnded ] If the session had been ended.
    #
    # @since 2.5.0
    def session_id
      if ended?
        raise Error::SessionEnded
      end

      # An explicit session will always have a session_id, because during
      # construction a server session must be provided. An implicit session
      # will not have a session_id until materialized, thus calls to
      # session_id might fail. An application should not have an opportunity
      # to experience this failure because an implicit session shouldn't be
      # accessible to applications due to its lifetime being constrained to
      # operation execution, which is done entirely by the driver.
      unless materialized?
        raise Error::SessionNotMaterialized
      end

      @server_session.session_id
    end

    # @return [ Server | nil ] The server (which should be a mongos) that this
    #   session is pinned to, if any.
    #
    # @api private
    attr_reader :pinned_server

    # @return [ Integer | nil ] The connection global id that this session is pinned to,
    #   if any.
    #
    # @api private
    attr_reader :pinned_connection_global_id

    # @return [ BSON::Document | nil ] Recovery token for the sharded
    #   transaction being executed on this session, if any.
    #
    # @api private
    attr_accessor :recovery_token

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
    # @deprecated
    SESSIONS_NOT_SUPPORTED = 'Sessions are not supported by the connected servers.'.freeze
    # Note: SESSIONS_NOT_SUPPORTED is used by Mongoid - do not remove from driver.

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

    # @api private
    UNLABELED_WRITE_CONCERN_CODES = [
      79,  # UnknownReplWriteConcern
      100, # CannotSatisfyWriteConcern,
    ].freeze

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
    # If there is an in-progress transaction on this session, the transaction
    # is aborted. The server session associated with this session is returned
    # to the server session pool. Finally, this session is marked ended and
    # is no longer usable.
    #
    # If this session is already ended, this method does nothing.
    #
    # Note that this method does not directly issue an endSessions command
    # to this server, contrary to what its name might suggest.
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
          rescue Mongo::Error, Error::AuthError
          end
        end
        if @server_session
          @client.cluster.session_pool.checkin(@server_session)
        end
      end
    ensure
      @server_session = nil
      @ended = true
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
      deadline = Utils.monotonic_time + 120
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
            log_warn("Aborting transaction due to #{e.class}: #{e}")
            abort_transaction
            transaction_in_progress = false
          end

          if Utils.monotonic_time >= deadline
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
              if Utils.monotonic_time >= deadline ||
                e.is_a?(Error::OperationFailure) && e.max_time_ms_expired?
              then
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
              if Utils.monotonic_time >= deadline
                transaction_in_progress = false
                raise
              end
              @state = NO_TRANSACTION_STATE
              next
            else
              transaction_in_progress = false
              raise
            end
          rescue Error::AuthError
            transaction_in_progress = false
            raise
          end
        end
      end

      # No official return value, but return true so that in interactive
      # use the method hints that it succeeded.
      true
    ensure
      if transaction_in_progress
        log_warn('with_transaction callback broke out of with_transaction loop, aborting transaction')
        begin
          abort_transaction
        rescue Error::OperationFailure, Error::InvalidTransactionOperation
        end
      end
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
    # @option options [ Integer ] :max_commit_time_ms The maximum amount of
    #   time to allow a single commitTransaction command to run, in milliseconds.
    # @option options [ Hash ] :read_concern The read concern options hash,
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

=begin
        # It would be handy to detect invalid read preferences here, but
        # some of the spec tests require later detection of invalid read prefs.
        # Maybe we can do this when lint mode is on.
        mode = options[:read] && options[:read][:mode].to_s
        if mode && mode != 'primary'
          raise Mongo::Error::InvalidTransactionOperation.new(
            "read preference in a transaction must be primary (requested: #{mode})"
          )
        end
=end
      end

      if snapshot?
        raise Mongo::Error::SnapshotSessionTransactionProhibited
      end

      check_if_ended!

      if within_states?(STARTING_TRANSACTION_STATE, TRANSACTION_IN_PROGRESS_STATE)
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation::TRANSACTION_ALREADY_IN_PROGRESS)
      end

      unpin

      next_txn_num
      @txn_options = (@options[:default_transaction_options] || {}).merge(options || {})

      if txn_write_concern && !WriteConcern.get(txn_write_concern).acknowledged?
        raise Mongo::Error::InvalidTransactionOperation.new(
          Mongo::Error::InvalidTransactionOperation::UNACKNOWLEDGED_WRITE_CONCERN)
      end

      @state = STARTING_TRANSACTION_STATE
      @already_committed = false

      # This method has no explicit return value.
      # We could return nil here but true indicates to the user that the
      # operation succeeded. This is intended for interactive use.
      # Note that the return value is not documented.
      true
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
      QueryCache.clear
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

          context = Operation::Context.new(client: @client, session: self)
          write_with_retry(write_concern, ending_transaction: true,
            context: context,
          ) do |connection, txn_num, context|
            if context.retry?
              if write_concern
                wco = write_concern.options.merge(w: :majority)
                wco[:wtimeout] ||= 10000
                write_concern = WriteConcern.get(wco)
              else
                write_concern = WriteConcern.get(w: :majority, wtimeout: 10000)
              end
            end
            spec = {
              selector: { commitTransaction: 1 },
              db_name: 'admin',
              session: self,
              txn_num: txn_num,
              write_concern: write_concern,
            }
            Operation::Command.new(spec).execute_with_connection(connection, context: context)
          end
        end
      ensure
        @state = TRANSACTION_COMMITTED_STATE
        @committing_transaction = false
      end

      # No official return value, but return true so that in interactive
      # use the method hints that it succeeded.
      true
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
      QueryCache.clear

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
          @aborting_transaction = true
          context = Operation::Context.new(client: @client, session: self)
          write_with_retry(txn_options[:write_concern],
            ending_transaction: true, context: context,
          ) do |connection, txn_num, context|
            begin
              Operation::Command.new(
                selector: { abortTransaction: 1 },
                db_name: 'admin',
                session: self,
                txn_num: txn_num
              ).execute_with_connection(connection, context: context)
            ensure
              unpin
            end
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
      ensure
        @aborting_transaction = false
      end

      # No official return value, but return true so that in interactive
      # use the method hints that it succeeded.
      true
    end

    # @api private
    def starting_transaction?
      within_states?(STARTING_TRANSACTION_STATE)
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

    # @return [ true | false ] Whether the session is currently aborting a
    #   transaction.
    #
    # @api private
    def aborting_transaction?
      !!@aborting_transaction
    end

    # Pins this session to the specified server, which should be a mongos.
    #
    # @param [ Server ] server The server to pin this session to.
    #
    # @api private
    def pin_to_server(server)
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

    # Pins this session to the specified connection.
    #
    # @param [ Integer ] connection_global_id The global id of connection to pin
    # this session to.
    #
    # @api private
    def pin_to_connection(connection_global_id)
      if connection_global_id.nil?
        raise ArgumentError, 'Cannot pin to a nil connection id'
      end
      @pinned_connection_global_id = connection_global_id
    end

    # Unpins this session from the pinned server or connection,
    # if the session was pinned.
    #
    # @param [ Connection | nil ] connection Connection to unpin from.
    #
    # @api private
    def unpin(connection = nil)
      @pinned_server = nil
      @pinned_connection_global_id = nil
      connection.unpin unless connection.nil?
    end

    # Unpins this session from the pinned server or connection, if the session was pinned
    # and the specified exception instance and the session's transaction state
    # require it to be unpinned.
    #
    # The exception instance should already have all of the labels set on it
    # (both client- and server-side generated ones).
    #
    # @param [ Error ] error The exception instance to process.
    # @param [ Connection | nil ] connection Connection to unpin from.
    #
    # @api private
    def unpin_maybe(error, connection = nil)
      if !within_states?(Session::NO_TRANSACTION_STATE) &&
        error.label?('TransientTransactionError')
      then
        unpin(connection)
      end

      if committing_transaction? &&
        error.label?('UnknownTransactionCommitResult')
      then
        unpin(connection)
      end
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
            c[:readConcern ] = Options::Mapper.transform_values_to_strings(rc)
          end
        end

        # We need to send the read concern level as a string rather than a symbol.
        if c[:readConcern]
          c[:readConcern] = Options::Mapper.transform_values_to_strings(c[:readConcern])
        end

        if c[:commitTransaction]
          if max_time_ms = txn_options[:max_commit_time_ms]
            c[:maxTimeMS] = max_time_ms
          end
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
      return unless in_transaction?
      return unless command['$readPreference']

      mode = command['$readPreference']['mode'] || command['$readPreference'][:mode]

      if mode && mode != 'primary'
        raise Mongo::Error::InvalidTransactionOperation.new(
          "read preference in a transaction must be primary (requested: #{mode})"
        )
      end
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

    # Validate the session for use by the specified client.
    #
    # The session must not be ended and must have been created by a client
    # with the same cluster as the client that the session is to be used with.
    #
    # @param [ Client ] client The client the session is to be used with.
    #
    # @return [ Session ] self, if the session is valid.
    #
    # @raise [ Mongo::Error::InvalidSession ] Exception raised if the session is not valid.
    #
    # @since 2.5.0
    # @api private
    def validate!(client)
      check_matching_cluster!(client)
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

    # If not already set, populate a session objects's server_session by
    # checking out a session from the session pool.
    #
    # @return [ Session ] Self.
    #
    # @api private
    def materialize_if_needed
      if ended?
        raise Error::SessionEnded
      end

      return unless implicit? && !@server_session

      @server_session = cluster.session_pool.checkout

      self
    end

    # @api private
    def materialized?
      if ended?
        raise Error::SessionEnded
      end

      !@server_session.nil?
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

    # @api private
    attr_accessor :snapshot_timestamp

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
      txn_options[:read_concern] || @client.read_concern
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
      txn_options[:write_concern] ||
        (@client.write_concern && @client.write_concern.options)
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

    def check_matching_cluster!(client)
      if @client.cluster != client.cluster
        raise Mongo::Error::InvalidSession.new(MISMATCHED_CLUSTER_ERROR_MSG)
      end
    end
  end
end
