# Test event subscriber.
#
# @since 2.5.0
class EventSubscriber

  module Impl

    # The started events.
    #
    # @since 2.5.0
    attr_reader :started_events

    # The succeeded events.
    #
    # @since 2.5.0
    attr_reader :succeeded_events

    # The failed events.
    #
    # @since 2.5.0
    attr_reader :failed_events

    # Cache the succeeded event.
    #
    # @param [ Event ] event The event.
    #
    # @since 2.5.0
    def succeeded(event)
      @mutex.synchronize do
        succeeded_events.push(event)
      end
    end

    # Cache the started event.
    #
    # @param [ Event ] event The event.
    #
    # @since 2.5.0
    def started(event)
      @mutex.synchronize do
        started_events.push(event)
      end
    end

    # Cache the failed event.
    #
    # @param [ Event ] event The event.
    #
    # @since 2.5.0
    def failed(event)
      @mutex.synchronize do
        failed_events.push(event)
      end
    end

    # Clear all cached events.
    #
    # @since 2.5.1
    def clear_events!
      @started_events = []
      @succeeded_events = []
      @failed_events = []
      self
    end

    def initialize
      @mutex = Mutex.new
      clear_events!
    end
  end

  include Impl

  class << self
    include Impl
    public :initialize
  end
end
