# frozen_string_literal: true
# rubocop:todo all

$sdam_formatter_lock = Mutex.new

module SdamFormatterIntegration
  def log_entries
    @log_entries ||= []
  end
  module_function :log_entries

  def clear_log_entries
    @log_entries = []
  end
  module_function :clear_log_entries

  def assign_log_entries(example_id)
    $sdam_formatter_lock.synchronize do
      @log_entries_by_example_id ||= {}
      @log_entries_by_example_id[example_id] ||= []
      @log_entries_by_example_id[example_id] += log_entries
      clear_log_entries
    end
  end
  module_function :assign_log_entries

  def example_log_entries(example_id)
    $sdam_formatter_lock.synchronize do
      @log_entries_by_example_id ||= {}
      @log_entries_by_example_id[example_id]
    end
  end
  module_function :example_log_entries

  def subscribe
    topology_opening_subscriber = TopologyOpeningLogSubscriber.new
    server_opening_subscriber = ServerOpeningLogSubscriber.new
    server_description_changed_subscriber = ServerDescriptionChangedLogSubscriber.new
    topology_changed_subscriber = TopologyChangedLogSubscriber.new
    server_closed_subscriber = ServerClosedLogSubscriber.new
    topology_closed_subscriber = TopologyClosedLogSubscriber.new

    Mongo::Monitoring::Global.subscribe(Mongo::Monitoring::TOPOLOGY_OPENING,
      topology_opening_subscriber)
    Mongo::Monitoring::Global.subscribe(Mongo::Monitoring::SERVER_OPENING,
      server_opening_subscriber)
    Mongo::Monitoring::Global.subscribe(Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED,
      server_description_changed_subscriber)
    Mongo::Monitoring::Global.subscribe(Mongo::Monitoring::TOPOLOGY_CHANGED,
      topology_changed_subscriber)
    Mongo::Monitoring::Global.subscribe(Mongo::Monitoring::SERVER_CLOSED,
      server_closed_subscriber)
    Mongo::Monitoring::Global.subscribe(Mongo::Monitoring::TOPOLOGY_CLOSED,
      topology_closed_subscriber)
  end
  module_function :subscribe

  class SDAMLogSubscriber
    def succeeded(event)
      SdamFormatterIntegration.log_entries <<
        Time.now.strftime('%Y-%m-%d %H:%M:%S.%L %z') + ' | ' + format_event(event)
    end
  end

  class TopologyOpeningLogSubscriber < SDAMLogSubscriber
    private

    def format_event(event)
      "Topology type '#{event.topology.display_name}' initializing."
    end
  end

  class ServerOpeningLogSubscriber < SDAMLogSubscriber
    private

    def format_event(event)
      "Server #{event.address} initializing."
    end
  end

  class ServerDescriptionChangedLogSubscriber < SDAMLogSubscriber
    private

    def format_event(event)
      "Server description for #{event.address} changed from " +
      "'#{event.previous_description.server_type}' to '#{event.new_description.server_type}'."
    end
  end

  class TopologyChangedLogSubscriber < SDAMLogSubscriber
    private

    def format_event(event)
      if event.previous_topology != event.new_topology
        "Topology type '#{event.previous_topology.display_name}' changed to " +
        "type '#{event.new_topology.display_name}'."
      else
        "There was a change in the members of the '#{event.new_topology.display_name}' " +
        "topology."
      end
    end
  end

  class ServerClosedLogSubscriber < SDAMLogSubscriber
    private

    def format_event(event)
      "Server #{event.address} connection closed."
    end
  end

  class TopologyClosedLogSubscriber < SDAMLogSubscriber
    private

    def format_event(event)
      "Topology type '#{event.topology.display_name}' closed."
    end
  end
end
