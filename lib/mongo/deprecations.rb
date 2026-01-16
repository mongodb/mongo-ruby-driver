# frozen_string_literal: true

require 'mongo/loggable'

module Mongo
  # Used for reporting deprecated behavior in the driver. When it is possible
  # to detect that a deprecated feature is being used, a warning should be issued
  # through this module.
  #
  # The warning will be issued no more than once for that feature, regardless
  # of how many times Mongo::Deprecations.warn is called.
  #
  # @example Issue a deprecation warning.
  #   Mongo::Deprecations.warn(:old_feature, "The old_feature is deprecated, use new_feature instead.")
  #
  # @api private
  module Deprecations
    extend self

    include Mongo::Loggable

    # Mutex for synchronizing access to warned features.
    # @api private
    MUTEX = Thread::Mutex.new

    # Issue a warning about a deprecated feature. The warning is written to the
    # logger, and will not be written more than once per feature.
    def warn(feature, message)
      MUTEX.synchronize do
        return if _warned?(feature)

        _warned!(feature)
        log_warn("[DEPRECATION:#{feature}] #{message}")
      end
    end

    # Check if a warning for a given deprecated feature has already been issued.
    #
    # @param [ String | Symbol ] feature The deprecated feature.
    #
    # @return [ true | false ] If a warning has already been issued.
    def warned?(feature, prefix: false)
      MUTEX.synchronize { _warned?(feature, prefix: prefix) }
    end

    # Mark that a warning for a given deprecated feature has been issued.
    #
    # @param [ String | Symbol ] feature The deprecated feature.
    def warned!(feature)
      MUTEX.synchronize { _warned!(feature) }
    end

    # Clears all memory of previously warned features.
    def clear!
      MUTEX.synchronize { warned_features reset: true }
      true
    end

    private

    def warned_features(reset: false)
      @warned_features = nil if reset
      @warned_features ||= Set.new
    end

    def _warned?(feature, prefix: false)
      if prefix
        warned_features.any? { |f| feature.to_s.start_with?(f) }
      else
        warned_features.include?(feature.to_s)
      end
    end

    def _warned!(feature)
      warned_features.add(feature.to_s)
    end
  end
end
