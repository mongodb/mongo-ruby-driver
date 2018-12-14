class JsonExtFormatter < RSpec::Core::Formatters::JsonFormatter
  RSpec::Core::Formatters.register self, :message, :dump_summary, :dump_profile, :stop, :seed, :close

  def format_example(example)
    super.tap do |hash|
      hash[:sdam_log_entries] = SdamFormatterIntegration.example_log_entries(example.id)
    end
  end
end
