# frozen_string_literal: true

module Mrss
  # Test event subscriber.
  class EventSubscriber

    # The mappings of event names to types.
    MAPPINGS = {
      'topology_opening_event' => Mongo::Monitoring::Event::TopologyOpening,
      'topology_description_changed_event' => Mongo::Monitoring::Event::TopologyChanged,
      'topology_closed_event' => Mongo::Monitoring::Event::TopologyClosed,
      'server_opening_event' => Mongo::Monitoring::Event::ServerOpening,
      'server_description_changed_event' => Mongo::Monitoring::Event::ServerDescriptionChanged,
      'server_closed_event' => Mongo::Monitoring::Event::ServerClosed
    }.freeze

    attr_reader :all_events

    attr_reader :started_events

    attr_reader :succeeded_events

    attr_reader :failed_events

    attr_reader :published_events

    # @param [ String ] name Optional name for the event subscriber.
    def initialize(name: nil)
      @mutex = Mutex.new
      clear_events!
      @name = name
    end

    def to_s
      %Q`#<EventSubscriber:#{@name ? "\"#{@name}\"" : '%x' % object_id} \
  started=#{started_events.length} \
  succeeded=#{succeeded_events.length} \
  failed=#{failed_events.length} \
  published=#{published_events.length}>`
    end

    alias :inspect :to_s

    # Event retrieval

    def select_started_events(cls)
      started_events.select do |event|
        event.is_a?(cls)
      end
    end

    def select_succeeded_events(cls)
      succeeded_events.select do |event|
        event.is_a?(cls)
      end
    end

    def select_completed_events(*classes)
      (succeeded_events + failed_events).select do |event|
        classes.any? { |c| c === event }
      end
    end

    def select_published_events(cls)
      published_events.select do |event|
        event.is_a?(cls)
      end
    end

    # Filters command started events for the specified command name.
    def command_started_events(command_name)
      started_events.select do |event|
        event.command[command_name]
      end
    end

    def non_auth_command_started_events
      started_events.reject do |event|
        %w(authenticate getnonce saslSstart saslContinue).any? do |cmd|
          event.command[cmd]
        end
      end
    end

    # Locates command stated events for the specified command name,
    # asserts that there is exactly one such event, and returns it.
    def single_command_started_event(command_name, include_auth: false, database_name: nil)
      events = if include_auth
                 started_events
               else
                 non_auth_command_started_events
               end
      get_one_event(events, command_name, 'started', database_name: database_name)
    end

    # Locates command succeeded events for the specified command name,
    # asserts that there is exactly one such event, and returns it.
    def single_command_succeeded_event(command_name, database_name: nil)
      get_one_event(succeeded_events, command_name, 'succeeded', database_name: database_name)
    end

    def get_one_event(events, command_name, kind, database_name: nil)
      events = events.select do |event|
        event.command_name == command_name and
        database_name.nil? || database_name == event.database_name
      end
      if events.length != 1
        raise "Expected a single '#{command_name}' #{kind} event#{database_name ? " for '#{database_name}'" : ''} but we have #{events.length}"
      end
      events.first
    end

    # Get the first succeeded event published for the name, and then delete it.
    #
    # @param [ String ] name The event name.
    #
    # @return [ Event ] The matching event.
    def first_event(name)
      cls = MAPPINGS[name]
      if cls.nil?
        raise ArgumentError, "Bogus event name #{name}"
      end
      matching = succeeded_events.find do |event|
        cls === event
      end
      succeeded_events.delete(matching)
      matching
    end

    # Event recording

    # Cache the started event.
    #
    # @param [ Event ] event The event.
    def started(event)
      @mutex.synchronize do
        started_events << event
        all_events << event
      end
    end

    # Cache the succeeded event.
    #
    # @param [ Event ] event The event.
    def succeeded(event)
      @mutex.synchronize do
        succeeded_events << event
        all_events << event
      end
    end

    # Cache the failed event.
    #
    # @param [ Event ] event The event.
    def failed(event)
      @mutex.synchronize do
        failed_events << event
        all_events << event
      end
    end

    def published(event)
      @mutex.synchronize do
        published_events << event
        all_events << event
      end
    end

    # Clear all cached events.
    def clear_events!
      @all_events = []
      @started_events = []
      @succeeded_events = []
      @failed_events = []
      @published_events = []
      self
    end
  end
  # Only handles succeeded events correctly.
  class PhasedEventSubscriber < EventSubscriber
    def initialize
      super
      @phase_events = {}
    end

    def phase_finished(phase_index)
      @phase_events[phase_index] = succeeded_events
      @succeeded_events = []
    end

    def phase_events(phase_index)
      @phase_events[phase_index]
    end

    def event_count
      @phase_events.inject(0) do |sum, event|
        sum + event.length
      end
    end
  end

  class VerboseEventSubscriber < EventSubscriber
    %w(started succeeded failed published).each do |meth|
      define_method(meth) do |event|
        puts event.summary
        super(event)
      end
    end
  end
end
