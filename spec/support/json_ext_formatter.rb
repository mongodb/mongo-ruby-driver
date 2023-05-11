# frozen_string_literal: true
# rubocop:todo all

class JsonExtFormatter < RSpec::Core::Formatters::JsonFormatter
  RSpec::Core::Formatters.register self, :message,
    :dump_summary, :dump_profile, :stop, :seed, :close

  def format_example(example)
    super.tap do |hash|
      # Time format is chosen to be the same as driver's log entries
      hash[:started_at] = example.execution_result.started_at.strftime('%Y-%m-%d %H:%M:%S.%L %z')
      hash[:finished_at] = example.execution_result.finished_at.strftime('%Y-%m-%d %H:%M:%S.%L %z')
      hash[:sdam_log_entries] = SdamFormatterIntegration.example_log_entries(example.id)
    end
  end
end
