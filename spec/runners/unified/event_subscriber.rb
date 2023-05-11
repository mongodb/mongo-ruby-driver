# frozen_string_literal: true
# rubocop:todo all

require 'mrss/event_subscriber'

module Unified

  class EventSubscriber < Mrss::EventSubscriber
    def ignore_commands(command_names)
      @ignore_commands = command_names
    end

    def wanted_events(observe_sensitive = false)
      events = all_events.select do |event|
        kind = event.class.name.sub(/.*::/, '').sub('Command', '').gsub(/([A-Z])/) { "_#{$1}" }.sub(/^_/, '').downcase.to_sym
        @wanted_events[kind]
      end.select do |event|
        if event.respond_to?(:command_name)
          event.command_name != 'configureFailPoint' &&
            if @ignore_commands
              !@ignore_commands.include?(event.command_name)
            else
              true
            end
        else
          true
        end
      end
      if observe_sensitive
        events
      else
        events.reject do |event|
          if event.respond_to?(:command_name)
            # event could be a command started event or command succeeded event
            command = event.respond_to?(:command) ? event.command : event.started_event.command
            %w(authenticate getnonce saslStart saslContinue).include?(event.command_name) ||
              # if the command is empty that means we used speculativeAuth and we should
              # reject the event.
              (%w(hello ismaster isMaster).include?(event.command_name) && command.empty?)
          end
        end
      end
    end

    def add_wanted_events(kind)
      @wanted_events ||= {}
      @wanted_events[kind] = true
    end
  end

  class StoringEventSubscriber
    def initialize(&block)
      @handler = block
    end

    def started(event)
      @handler.call(
        'name' => event.class.name.sub(/.*::/, '') + 'Event',
        'commandName' => event.command_name,
        'databaseName' => event.database_name,
        'observedAt' => Time.now.to_f,
        'address' => event.address.seed,
        'requestId' => event.request_id,
        'operationId' => event.operation_id,
        'connectionId' => event.connection_id,
      )
    end

    def succeeded(event)
      @handler.call(
        'name' => event.class.name.sub(/.*::/, '') + 'Event',
        'commandName' => event.command_name,
        'duration' => event.duration,
        'observedAt' => Time.now.to_f,
        'address' => event.address.seed,
        'requestId' => event.request_id,
        'operationId' => event.operation_id,
      )
    end

    def failed(event)
      @handler.call(
        'name' => event.class.name.sub(/.*::/, '') + 'Event',
        'commandName' => event.command_name,
        'duration' => event.duration,
        'failure' => event.failure,
        'observedAt' => Time.now.to_f,
        'address' => event.address.seed,
        'requestId' => event.request_id,
        'operationId' => event.operation_id,
      )
    end

    def published(event)
      payload = {
        'name' => event.class.name.sub(/.*::/, '') + 'Event',
        'observedAt' => Time.now.to_f,
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
