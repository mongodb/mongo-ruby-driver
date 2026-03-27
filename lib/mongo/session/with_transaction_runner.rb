# frozen_string_literal: true

module Mongo
  class Session
    # Owns the retry loop state and logic for Session#with_transaction.
    #
    # Control-flow contract:
    #   return     — step succeeded, continue
    #   raise      — unrecoverable error, propagate to caller
    #   throw :retry — restart the transaction loop from the top
    #
    # @api private
    class WithTransactionRunner
      BACKOFF_INITIAL = 0.005
      BACKOFF_MAX     = 0.5
      private_constant :BACKOFF_INITIAL, :BACKOFF_MAX

      def initialize(session, options)
        @session  = session
        @options  = options
        @csot     = !session.with_transaction_timeout_ms.nil?
        csot_deadline = session.with_transaction_deadline
        # Non-CSOT: apply 120-second safety limit.
        # CSOT: use computed deadline (0 = infinite when timeout_ms: 0).
        @deadline = csot_deadline.nil? ? (Utils.monotonic_time + 120) : csot_deadline
        @last_error              = nil
        @transaction_attempt     = 0
        @overload_encountered    = false
        @overload_error_count    = 0
        @transaction_in_progress = false
      end

      private

      # -- Deadline helpers --------------------------------------------------

      def deadline_expired?
        @deadline.zero? ? false : Utils.monotonic_time >= @deadline
      end

      def backoff_would_exceed_deadline?(secs)
        @deadline.zero? ? false : Utils.monotonic_time + secs >= @deadline
      end

      # -- Backoff -----------------------------------------------------------

      def backoff_seconds_for_retry
        exponential = BACKOFF_INITIAL * (1.5**(@transaction_attempt - 1))
        Random.rand * [ exponential, BACKOFF_MAX ].min
      end

      # -- Timeout error -----------------------------------------------------

      # Raises TimeoutError (CSOT) or re-raises last_error (non-CSOT).
      # @csot mirrors the original `if @with_transaction_timeout_ms` check:
      # in Ruby, 0 is truthy, so timeout_ms: 0 (infinite CSOT) sets @csot = true
      # just as `if 0` evaluates to true — the semantics are identical.
      def make_timeout_error_from(last_error, message)
        raise Mongo::Error::TimeoutError, "#{message}: #{last_error}" if @csot

        raise last_error
      end

      # -- Overload tracking -------------------------------------------------

      # Updates @overload_encountered and @overload_error_count.
      # Does not raise or return a meaningful value.
      def track_overload(err)
        if err.label?('SystemOverloadedError')
          @overload_encountered = true
          @overload_error_count += 1
        elsif @overload_encountered
          @overload_error_count += 1
          @session.client.retry_policy.record_non_overload_retry_failure
        end
      end

      # -- execute_callback helpers ------------------------------------------

      # Aborts the transaction if it is currently active.
      # Clears the deadline first if it is already expired so that abort gets
      # a fresh timeout rather than the expired one.
      def abort_in_progress_transaction(err)
        return unless @session.within_states?(
          Session::STARTING_TRANSACTION_STATE,
          Session::TRANSACTION_IN_PROGRESS_STATE
        )

        @session.log_warn("Aborting transaction due to #{err.class}: #{err}")
        @session.clear_with_transaction_deadline! if @csot && deadline_expired?
        @session.abort_transaction
        @transaction_in_progress = false
      end

      # Raises if the deadline has passed.
      # In CSOT mode raises TimeoutError; in non-CSOT mode re-raises last_error.
      def raise_or_retry_on_deadline!(err)
        return unless deadline_expired?

        make_timeout_error_from(err, 'CSOT timeout expired during withTransaction callback')
      end

      # Handles the error from the callback.
      # Throws :retry for transient errors; re-raises everything else.
      def handle_transient_callback_error(err)
        raise err unless err.is_a?(Mongo::Error) && err.label?('TransientTransactionError')

        @last_error = err
        track_overload(err)
        throw :retry
      end

      # Runs the user's block; handles errors using the three helpers above.
      # rubocop:disable Lint/RescueException
      def execute_callback
        yield
      rescue Exception => e
        abort_in_progress_transaction(e)
        raise_or_retry_on_deadline!(e)
        handle_transient_callback_error(e)
      end
      # rubocop:enable Lint/RescueException

      # -- Pre-retry backoff -------------------------------------------------

      # Sleeps before the next transaction attempt, checking the deadline first.
      # Two branches: overload path (adaptive) and normal path (exponential).
      def pre_retry_backoff
        if @overload_encountered
          delay = @session.client.retry_policy.backoff_delay(@overload_error_count)
          if backoff_would_exceed_deadline?(delay)
            make_timeout_error_from(@last_error, 'CSOT timeout expired waiting to retry withTransaction')
          end
          raise @last_error unless @session.client.retry_policy.should_retry_overload?(@overload_error_count, delay)

          sleep(delay)
        else
          backoff = backoff_seconds_for_retry
          if backoff_would_exceed_deadline?(backoff)
            make_timeout_error_from(@last_error, 'CSOT timeout expired waiting to retry withTransaction')
          end
          sleep(backoff)
        end
      end

      # -- Commit helpers ----------------------------------------------------

      def build_commit_options
        @options ? { write_concern: @options[:write_concern] } : {}
      end

      # Escalates write concern to w: :majority for commit retries.
      # Note: wtimeout is NOT added here — commit_transaction handles that
      # internally when it detects a retry context.
      def escalate_write_concern!(opts)
        wc = case (v = opts[:write_concern])
             when WriteConcern::Base then v.options
             when nil                then {}
             else                         v
             end
        opts[:write_concern] = wc.merge(w: :majority)
      end

      # -- Ensure guard ------------------------------------------------------

      # Called from run's ensure. Aborts if a break/external Timeout escaped
      # the loop while a transaction was in progress.
      def abort_if_in_progress
        return unless @transaction_in_progress

        @session.log_warn(
          'with_transaction callback broke out of with_transaction loop, ' \
          'aborting transaction'
        )
        begin
          @session.abort_transaction
        rescue Error::OperationFailure::Family, Error::InvalidTransactionOperation
          # Ignore — transaction may already be in an inconsistent state.
        end
      end

      # -- Sleep wrapper (stubbable in tests) --------------------------------

      def sleep(secs)
        Kernel.sleep(secs)
      end
    end
  end
end
