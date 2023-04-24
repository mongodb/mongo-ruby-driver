# frozen_string_literal: true
# rubocop:todo all

module Mongo
  module Config

    # Encapsulates logic for setting options.
    module Options

      # Get the defaults or initialize a new empty hash.
      #
      # @return [ Hash ] The default options.
      def defaults
        @defaults ||= {}
      end

      # Define a configuration option with a default.
      #
      # @param [ Symbol ] name The name of the configuration option.
      # @param [ Hash ] options Extras for the option.
      #
      # @option options [ Object ] :default The default value.
      def option(name, options = {})
        defaults[name] = settings[name] = options[:default]

        class_eval do
          # log_level accessor is defined specially below
          define_method(name) do
            settings[name]
          end

          define_method("#{name}=") do |value|
            settings[name] = value
          end

          define_method("#{name}?") do
            !!send(name)
          end
        end
      end

      # Reset the configuration options to the defaults.
      #
      # @example Reset the configuration options.
      #   config.reset
      #
      # @return [ Hash ] The defaults.
      def reset
        settings.replace(defaults)
      end

      # Get the settings or initialize a new empty hash.
      #
      # @example Get the settings.
      #   options.settings
      #
      # @return [ Hash ] The setting options.
      def settings
        @settings ||= {}
      end
    end
  end
end
