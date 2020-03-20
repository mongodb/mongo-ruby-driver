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

    attr_reader :published_events

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

    # Filters command started events for the specified command name.
    def command_started_events(command_name)
      started_events.select do |event|
        event.command[command_name]
      end
    end

    # Locates command stated events for the specified command name,
    # asserts that there is exactly one such event, and returns it.
    def single_command_started_event(command_name)
      events = started_events.select do |event|
        event.command[command_name]
      end
      if events.length != 1
        raise "Expected a single #{command_name} event but we have #{events.length}"
      end
      events.first
    end

    def select_started_events(cls)
      @started_events.select do |event|
        event.is_a?(cls)
      end
    end

    def select_succeeded_events(cls)
      @succeeded_events.select do |event|
        event.is_a?(cls)
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

    def select_published_events(cls)
      @published_events.select do |event|
        event.is_a?(cls)
      end
    end

    def published(event)
      @mutex.synchronize do
        @published_events << event
      end
    end

    # Clear all cached events.
    #
    # @since 2.5.1
    def clear_events!
      @started_events = []
      @succeeded_events = []
      @failed_events = []
      @published_events = []
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
