# frozen_string_literal: true
# rubocop:todo all

module Mongo

  # @api private
  module Lint

    # Raises LintError if +obj+ is not of type +cls+.
    def assert_type(obj, cls)
      return unless enabled?
      unless obj.is_a?(cls)
        raise Error::LintError, "Expected #{obj} to be a #{cls}"
      end
    end
    module_function :assert_type

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

    # Validates the provided hash as a read concern object, per the
    # read/write concern specification
    # (https://github.com/mongodb/specifications/blob/master/source/read-write-concern/read-write-concern.rst#read-concern).
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
      unless read_concern.is_a?(Hash)
        raise Error::LintError, "Read concern is not a hash: #{read_concern}"
      end
      return if read_concern.empty?
      keys = read_concern.keys
      if read_concern.is_a?(BSON::Document)
        # Permits indifferent access
        allowed_keys = ['level']
      else
        # Does not permit indifferent access
        allowed_keys = [:level]
      end
      if keys != allowed_keys
        raise Error::LintError, "Read concern has invalid keys: #{keys.inspect}"
      end
      level = read_concern[:level]
      return if [:local, :available, :majority, :linearizable, :snapshot].include?(level)
      raise Error::LintError, "Read concern level is invalid: value must be a symbol: #{level.inspect}"
    end
    module_function :validate_read_concern_option

    def enabled?
      ENV['MONGO_RUBY_DRIVER_LINT'] && %w(1 yes true on).include?(ENV['MONGO_RUBY_DRIVER_LINT'].downcase)
    end
    module_function :enabled?
  end
end
