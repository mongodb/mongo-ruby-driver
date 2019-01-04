module Mongo
  # @api private
  module Lint
    def validate_underscore_read_preference(read_pref)
      return unless enabled?
      return if read_pref.nil?
      unless read_pref.is_a?(Hash)
        raise Error::LintError, "Read preference is not a hash: #{read_pref}"
      end
      validate_underscore_read_preference_mode(read_pref[:mode] || read_pref['mode'])
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
      return if read_pref.nil?
      unless read_pref.is_a?(Hash)
        raise Error::LintError, "Read preference is not a hash: #{read_pref}"
      end
      validate_camel_case_read_preference_mode(read_pref[:mode] || read_pref['mode'])
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
      ENV['MONGO_RUBY_DRIVER_LINT'] && %w(1 yes true on).include?(ENV['MONGO_RUBY_DRIVER_LINT'].downcase)
    end
    module_function :enabled?
  end
end
