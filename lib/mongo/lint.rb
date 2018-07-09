module Mongo
  # @api private
  module Lint
    def validate_underscore_read_preference(read_pref)
      return unless enabled?
      if read_pref
        validate_underscore_read_preference_mode(read_pref[:mode] || read_pref['mode'])
      end
    end
    module_function :validate_underscore_read_preference

    def validate_underscore_read_preference_mode(mode)
      return unless enabled?
      if mode
        unless %w(primary primary_preferred secondary secondary_preferred nearest).include?(mode.to_s)
          raise Error::LintError, "Invalid read preference mode: #{mode}"
        end
      end
    end
    module_function :validate_underscore_read_preference_mode

    def validate_camel_case_read_preference(read_pref)
      return unless enabled?
      if read_pref
        validate_camel_case_read_preference_mode(read_pref[:mode] || read_pref['mode'])
      end
    end
    module_function :validate_camel_case_read_preference

    def validate_camel_case_read_preference_mode(mode)
      return unless enabled?
      if mode
        unless %w(primary primaryPreferred secondary secondaryPreferred nearest).include?(mode.to_s)
          raise Error::LintError, "Invalid read preference mode: #{mode}"
        end
      end
    end
    module_function :validate_camel_case_read_preference_mode

    def enabled?
      ENV['MONGO_RUBY_DRIVER_LINT'] && %w(1 yes true).include?(ENV['MONGO_RUBY_DRIVER_LINT'].downcase)
    end
    module_function :enabled?
  end
end
