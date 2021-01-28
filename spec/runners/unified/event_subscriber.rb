require 'support/event_subscriber'

module Unified

  class EventSubscriber < ::EventSubscriber
    def ignore_commands(command_names)
      @ignore_commands = command_names
    end

    def wanted_events
      all_events.select do |event|
        kind = event.class.name.sub(/.*::/, '').sub('Command', '').downcase.to_sym
        @wanted_events[kind]
      end.select do |event|
        event.command_name != 'configureFailPoint' &&
          if @ignore_commands
            !@ignore_commands.include?(event.command_name)
          else
            true
          end
      end.reject do |event|
        %w(authenticate getnonce saslStart saslContinue).include?(event.command_name)
      end
    end

    def add_wanted_events(kind)
      @wanted_events ||= {}
      @wanted_events[kind] = true
    end
  end

  class StoringEventSubscriber
    def initialize(&block)
      @operations = {}
      @handler = block
    end

    def started(event)
      started_at = Time.now
      @operations[event.operation_id] = [event, started_at]
      @handler.call(
        'name' => event.class.name.sub(/.*::/, '') + 'Event',
        'commandName' => event.command_name,
        'startTime' => started_at.to_f,
        'address' => event.address.seed,
      )
    end

    def succeeded(event)
      started_event, started_at = @operations.delete(event.operation_id)
      raise "Started event for #{event.operation_id} not found" unless started_event
      @handler.call(
        'name' => event.class.name.sub(/.*::/, '') + 'Event',
        'commandName' => started_event.command_name,
        'duration' => event.duration,
        'startTime' => started_at.to_f,
        'address' => started_event.address.seed,
      )
    end

    def failed(event)
      started_event, started_at = @operations.delete(event.operation_id)
      raise "Started event for #{event.operation_id} not found" unless started_event
      @handler.call(
        'name' => event.class.name.sub(/.*::/, '') + 'Event',
        'commandName' => started_event.command_name,
        'duration' => event.duration,
        'failure' => event.failure,
        'startTime' => started_at.to_f,
        'address' => started_event.address.seed,
      )
    end

    def published(event)
      payload = {
        'name' => event.class.name.sub(/.*::/, '') + 'Event',
        'time' => Time.now.to_f,
        'address' => event.address.seed,
      }.tap do |entry|
        if event.respond_to?(:connection_id)
          entry['connectionId'] = event.connection_id
        end
        if event.respond_to?(:reason)
          entry['reason'] = event.reason
        end
      end
      @handler.call(payload)
    end
  end
end
