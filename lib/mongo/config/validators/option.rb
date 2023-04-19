# frozen_string_literal: true
# rubocop:todo all

module Mongo
  module Config
    module Validators

      # Validator for configuration options.
      #
      # @api private
      module Option
        extend self

        # Validate a configuration option.
        #
        # @example Validate a configuration option.
        #
        # @param [ String ] option The name of the option.
        def validate(option)
          unless Config.settings.keys.include?(option.to_sym)
            raise Mongo::Error::InvalidConfigOption.new(option)
          end
        end
      end
    end
  end
end
