# frozen_string_literal: true

module Mongo
  # A shared concern implementing settings and configuration for entities that
  # "host" (or spawn) cursors.
  #
  # The class or module that includes this concern must implement:
  #   * timeout_ms -- this must return either the operation level timeout_ms
  #       (if set) or an inherited timeout_ms from a hierarchically higher
  #       level (if any).
  module CursorHost
    # Returns the cursor associated with this view, if any.
    #
    # @return [ nil | Cursor ] The cursor, if any.
    #
    # @api private
    attr_reader :cursor

    # @return [ :cursor_lifetime | :iteration ] The timeout mode to be
    #   used by this object.
    attr_reader :timeout_mode

    # Ensure the timeout mode is appropriate for other options that
    # have been given.
    #
    # @param [ Hash ] options The options to inspect.
    # @param [ Array<Symbol> ] forbid The list of options to forbid for this
    #   class.
    #
    # @raise [ ArgumentError ] if inconsistent or incompatible options are
    #   detected.
    #
    # @api private
    # rubocop:disable Metrics
    def validate_timeout_mode!(options, forbid: [])
      forbid.each do |key|
        raise ArgumentError, "#{key} is not allowed here" if options.key?(key)
      end

      cursor_type = options[:cursor_type]
      timeout_mode = options[:timeout_mode]

      if timeout_ms
        # "Tailable cursors only support the ITERATION value for the
        # timeoutMode option. This is the default value and drivers MUST
        # error if the option is set to CURSOR_LIFETIME."
        if cursor_type
          timeout_mode ||= :iteration
          if timeout_mode == :cursor_lifetime
            raise ArgumentError, 'tailable cursors only support `timeout_mode: :iteration`'
          end

          # "Drivers MUST error if [the maxAwaitTimeMS] option is set,
          # timeoutMS is set to a non-zero value, and maxAwaitTimeMS is
          # greater than or equal to timeoutMS."
          max_await_time_ms = options[:max_await_time_ms] || 0
          if cursor_type == :tailable_await && max_await_time_ms >= timeout_ms
            raise ArgumentError, ':max_await_time_ms must not be >= :timeout_ms'
          end
        else
          # "For non-tailable cursors, the default value of timeoutMode
          # is CURSOR_LIFETIME."
          timeout_mode ||= :cursor_lifetime
        end
      elsif timeout_mode
        # "Drivers MUST error if timeoutMode is set and timeoutMS is not."
        raise ArgumentError, ':timeout_ms must be set if :timeout_mode is set'
      end

      if timeout_mode == :iteration && respond_to?(:write?) && write?
        raise ArgumentError, 'timeout_mode=:iteration is not supported for aggregation pipelines with $out or $merge'
      end

      # set it as an instance variable, rather than updating the options,
      # because if the cursor type changes (e.g. via #configure()), the new
      # View instance must be able to select a different default timeout_mode
      # if no timeout_mode was set initially.
      @timeout_mode = timeout_mode
    end
    # rubocop:enable Metrics
  end
end
