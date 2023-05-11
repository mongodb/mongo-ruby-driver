# frozen_string_literal: true
# rubocop:todo all

module Mongo
  class Error

    # This error is raised when a bad configuration option is attempted to be
    # set.
    class InvalidConfigOption < Error

      # Create the new error.
      #
      # @param [ Symbol, String ] name The attempted config option name.
      #
      # @api private
      def initialize(name)
        super("Invalid config option #{name}.")
      end
    end
  end
end
