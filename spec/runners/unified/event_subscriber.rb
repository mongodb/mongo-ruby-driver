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
end
