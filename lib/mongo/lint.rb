# frozen_string_literal: true

module Mongo
  # @api private
  module Lint
    # Raises LintError if +obj+ is not of type +cls+.
    def assert_type(obj, cls)
      return unless enabled?
      return if obj.is_a?(cls)

      raise Error::LintError, "Expected #{obj} to be a #{cls}"
    end
    module_function :assert_type

    def validate_underscore_read_preference(read_pref)
      return unless enabled?
      return if read_pref.nil?
      raise Error::LintError, "Read preference is not a hash: #{read_pref}" unless read_pref.is_a?(Hash)

      validate_underscore_read_preference_mode(read_pref[:mode] || read_pref['mode'])
    end
    module_function :validate_underscore_read_preference

    def validate_underscore_read_preference_mode(mode)
      return unless enabled?

      return unless mode
      return if %w[primary primary_preferred secondary secondary_preferred nearest].include?(mode.to_s)

      raise Error::LintError, "Invalid read preference mode: #{mode}"
    end
    module_function :validate_underscore_read_preference_mode

    def validate_camel_case_read_preference(read_pref)
      return unless enabled?
      return if read_pref.nil?
      raise Error::LintError, "Read preference is not a hash: #{read_pref}" unless read_pref.is_a?(Hash)

      validate_camel_case_read_preference_mode(read_pref[:mode] || read_pref['mode'])
    end
    module_function :validate_camel_case_read_preference

    def validate_camel_case_read_preference_mode(mode)
      return unless enabled?

      return unless mode
      return if %w[primary primaryPreferred secondary secondaryPreferred nearest].include?(mode.to_s)

      raise Error::LintError, "Invalid read preference mode: #{mode}"
    end
    module_function :validate_camel_case_read_preference_mode

    # Validates the provided hash as a read concern object, per the
    # read/write concern specification
    # (https://github.com/mongodb/specifications/blob/master/source/read-write-concern/read-write-concern.md#read-concern).
    #
    # This method also accepts nil as input for convenience.
    #
    # The read concern document as sent to the server may include
    # additional fields, for example afterClusterTime. These fields
    # are generated internally by the driver and cannot be specified by
    # the user (and would potentially lead to incorrect behavior if they
    # were specified by the user), hence this method prohibits them.
    #
    # @param [ Hash ] read_concern The read concern options hash,
    #   with the following optional keys:
    #   - *:level* -- the read preference level as a symbol; valid values
    #      are *:local*, *:majority*, and *:snapshot*
    #
    # @raise [ Error::LintError ] If the validation failed.
    def validate_read_concern_option(read_concern)
      return unless enabled?
      return if read_concern.nil?
      raise Error::LintError, "Read concern is not a hash: #{read_concern}" unless read_concern.is_a?(Hash)
      return if read_concern.empty?

      keys = read_concern.keys
      allowed_keys = if read_concern.is_a?(BSON::Document)
                       # Permits indifferent access
                       [ 'level' ]
                     else
                       # Does not permit indifferent access
                       [ :level ]
                     end
      raise Error::LintError, "Read concern has invalid keys: #{keys.inspect}" if keys != allowed_keys

      level = read_concern[:level]
      return if %i[local available majority linearizable snapshot].include?(level)

      raise Error::LintError, "Read concern level is invalid: value must be a symbol: #{level.inspect}"
    end
    module_function :validate_read_concern_option

    def enabled?
      ENV['MONGO_RUBY_DRIVER_LINT'] && %w[1 yes true on].include?(ENV['MONGO_RUBY_DRIVER_LINT'].downcase)
    end
    module_function :enabled?
  end
end
