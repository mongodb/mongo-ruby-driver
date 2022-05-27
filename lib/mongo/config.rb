# frozen_string_literal: true

require "mongo/config/options"
require "mongo/config/validators/option"

module Mongo

  # This module defines configuration options for Mongo.
  module Config
    extend Forwardable
    extend Options
    extend self

    option :validate_update_replace, default: false

    # Set the configuration options.
    #
    # @example Set the options.
    #   config.options = { validate_update_replace: true }
    #
    # @param [ Hash ] options The configuration options.
    def options=(options)
      if options
        options.each_pair do |option, value|
          Validators::Option.validate(option)
          send("#{option}=", value)
        end
      end
    end
  end
end
