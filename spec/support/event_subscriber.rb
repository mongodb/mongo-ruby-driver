# Test event subscriber.
#
# @since 2.5.0
class EventSubscriber

  class << self

    # The started events.
    #
    # @since 2.5.0
    def started_events
      @started_events ||= []
    end

    # The succeeded events.
    #
    # @since 2.5.0
    def succeeded_events
      @succeeded_events ||= []
    end

    # The failed events.
    #
    # @since 2.5.0
    def failed_events
      @failed_events ||= []
    end

    # Cache the succeeded event.
    #
    # @param [ Event ] event The event.
    #
    # @since 2.5.0
    def succeeded(event)
      succeeded_events.push(event)
    end

    # Cache the started event.
    #
    # @param [ Event ] event The event.
    #
    # @since 2.5.0
    def started(event)
      started_events.push(event)
    end

    # Cache the failed event.
    #
    # @param [ Event ] event The event.
    #
    # @since 2.5.0
    def failed(event)
      failed_events.push(event)
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
  end
end
