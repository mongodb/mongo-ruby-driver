# frozen_string_literal: true
# rubocop:todo all

require "mongo/config/options"
require "mongo/config/validators/option"

module Mongo

  # This module defines configuration options for Mongo.
  #
  # @api private
  module Config
    extend Forwardable
    extend Options
    extend self

    # When this flag is off, an aggregation done on a view will be executed over
    # the documents included in that view, instead of all documents in the
    # collection. When this flag is on, the view filter is ignored.
    option :broken_view_aggregate, default: true

    # When this flag is set to false, the view options will be correctly
    # propagated to readable methods.
    option :broken_view_options, default: true

    # When this flag is set to true, the update and replace methods will
    # validate the parameters and raise an error if they are invalid.
    option :validate_update_replace, default: false

    # Set the configuration options.
    #
    # @example Set the options.
    #   config.options = { validate_update_replace: true }
    #
    # @param [ Hash ] options The configuration options.
    def options=(options)
      options.each_pair do |option, value|
        Validators::Option.validate(option)
        send("#{option}=", value)
      end
    end
  end
end
