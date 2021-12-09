# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class URI

    # Performs mapping between URI options and Ruby options.
    #
    # This class contains:
    #
    # - The mapping defining how URI options are converted to Ruby options.
    # - The mapping from downcased URI option names to canonical-cased URI
    #   option names.
    # - Methods to perform conversion of URI option values to Ruby option
    #   values (the convert_* methods). These generally warn and return nil
    #   when input given is invalid.
    # - Methods to perform conversion of Ruby option values to standardized
    #   MongoClient options (revert_* methods). These assume the input is valid
    #   and generally do not perform validation.
    #
    # URI option names are case insensitive. Ruby options are specified as
    # symbols (though in Client options use indifferent access).
    #
    # @api private
    class OptionsMapper

      include Loggable

      # Instantates the options mapper.
      #
      # @option opts [ Logger ] :logger A custom logger to use.
      def initialize(**opts)
        @options = opts
      end

      # @return [ Hash ] The options.
      attr_reader :options

      # Adds an option to the uri options hash.
      #
      #   Acquires a target for the option based on group.
      #   Transforms the value.
      #   Merges the option into the target.
      #
      # @param [ String ] key URI option name.
      # @param [ String ] value The value of the option.
      # @param [ Hash ] uri_options The base option target.
      def add_uri_option(key, value, uri_options)
        strategy = URI_OPTION_MAP[key.downcase]
        if strategy.nil?
          log_warn("Unsupported URI option '#{key}' on URI '#{@string}'. It will be ignored.")
          return
        end

        group = strategy[:group]
        target = if group
          uri_options[group] || {}
        else
          uri_options
        end
        value = apply_transform(key, value, strategy[:type])
        # Sometimes the value here would be nil, for example if we are processing
        # read preference tags or auth mechanism properties and all of the
        # data within is invalid. Ignore such options.
        unless value.nil?
          merge_uri_option(target, value, strategy[:name])
        end

        if group && !target.empty? && !uri_options.key?(group)
          uri_options[group] = target
        end
      end

      def smc_to_ruby(opts)
        uri_options = {}

        opts.each do |key, value|
          strategy = URI_OPTION_MAP[key.downcase]
          if strategy.nil?
            log_warn("Unsupported URI option '#{key}' on URI '#{@string}'. It will be ignored.")
            return
          end

          group = strategy[:group]
          target = if group
            uri_options[group] || {}
          else
            uri_options
          end

          if key == 'readConcernLevel'
            value = value.to_sym
          end

          #value = apply_transform(key, value, strategy[:type])
          # Sometimes the value here would be nil, for example if we are processing
          # read preference tags or auth mechanism properties and all of the
          # data within is invalid. Ignore such options.
          unless value.nil?
            merge_uri_option(target, value, strategy[:name])
          end

          if group && !target.empty? && !uri_options.key?(group)
            uri_options[group] = target
          end
        end

        #p uri_options
        uri_options
      end

      # Converts Ruby options provided to "standardized MongoClient options".
      #
      # @param [ Hash ] opts Ruby options to convert.
      #
      # @return [ Hash ] Standardized MongoClient options.
      def ruby_to_smc(opts)
        rv = {}
        URI_OPTION_MAP.each do |uri_key, spec|
          if spec[:group]
            v = opts[spec[:group]]
            v = v && v[spec[:name]]
          else
            v = opts[spec[:name]]
          end
          unless v.nil?
            if spec[:type]
              v = send("revert_#{spec[:type]}", v)
            end
            canonical_key = URI_OPTION_CANONICAL_NAMES[uri_key]
            unless canonical_key
              raise ArgumentError, "Option #{uri_key} is not known"
            end
            rv[canonical_key] = v
          end
        end
        # For options that default to true, remove the value if it is true.
        %w(retryReads retryWrites).each do |k|
          if rv[k]
            rv.delete(k)
          end
        end
        # Remove auth source when it is $external for mechanisms that default
        # (or require) that auth source.
        if %w(MONGODB-AWS).include?(rv['authMechanism']) && rv['authSource'] == '$external'
          rv.delete('authSource')
        end
        # ssl and tls are aliases, remove ssl ones
        rv.delete('ssl')
        # TODO remove authSource if it is the same as the database,
        # requires this method to know the database specified in the client.
        rv
      end

      private

      # Applies URI value transformation by either using the default cast
      # or a transformation appropriate for the given type.
      #
      # @param key [String] URI option name.
      # @param value [String] The value to be transformed.
      # @param type [Symbol] The transform method.
      def apply_transform(key, value, type)
        if type
          send("convert_#{type}", key, value)
        else
          value
        end
      end

      # Merges a new option into the target.
      #
      # If the option exists at the target destination the merge will
      # be an addition.
      #
      # Specifically required to append an additional tag set
      # to the array of tag sets without overwriting the original.
      #
      # @param target [Hash] The destination.
      # @param value [Object] The value to be merged.
      # @param name [Symbol] The name of the option.
      def merge_uri_option(target, value, name)
        if target.key?(name)
          if REPEATABLE_OPTIONS.include?(name)
            target[name] += value
          else
            log_warn("Repeated option key: #{name}.")
          end
        else
          target.merge!(name => value)
        end
      end

      # Hash for storing map of URI option parameters to conversion strategies
      URI_OPTION_MAP = {}

      # @return [ Hash<String, String> ] Map from lowercased to canonical URI
      #   option names.
      URI_OPTION_CANONICAL_NAMES = {}

      # Simple internal dsl to register a MongoDB URI option in the URI_OPTION_MAP.
      #
      # @param uri_key [String] The MongoDB URI option to register.
      # @param name [Symbol] The name of the option in the driver.
      # @param extra [Hash] Extra options.
      #   * :group [Symbol] Nested hash where option will go.
      #   * :type [Symbol] Name of function to transform value.
      def self.uri_option(uri_key, name, **extra)
        URI_OPTION_MAP[uri_key.downcase] = { name: name }.update(extra)
        URI_OPTION_CANONICAL_NAMES[uri_key.downcase] = uri_key
      end

      # Replica Set Options
      uri_option 'replicaSet', :replica_set

      # Timeout Options
      uri_option 'connectTimeoutMS', :connect_timeout, type: :ms
      uri_option 'socketTimeoutMS', :socket_timeout, type: :ms
      uri_option 'serverSelectionTimeoutMS', :server_selection_timeout, type: :ms
      uri_option 'localThresholdMS', :local_threshold, type: :ms
      uri_option 'heartbeatFrequencyMS', :heartbeat_frequency, type: :ms
      uri_option 'maxIdleTimeMS', :max_idle_time, type: :ms

      # Write Options
      uri_option 'w', :w, group: :write_concern, type: :w
      uri_option 'journal', :j, group: :write_concern, type: :bool
      uri_option 'fsync', :fsync, group: :write_concern, type: :bool
      uri_option 'wTimeoutMS', :wtimeout, group: :write_concern, type: :integer

      # Read Options
      uri_option 'readPreference', :mode, group: :read, type: :read_mode
      uri_option 'readPreferenceTags', :tag_sets, group: :read, type: :read_tags
      uri_option 'maxStalenessSeconds', :max_staleness, group: :read, type: :max_staleness

      # Pool options
      uri_option 'minPoolSize', :min_pool_size, type: :integer
      uri_option 'maxPoolSize', :max_pool_size, type: :integer
      uri_option 'waitQueueTimeoutMS', :wait_queue_timeout, type: :ms

      # Security Options
      uri_option 'ssl', :ssl, type: :repeated_bool
      uri_option 'tls', :ssl, type: :repeated_bool
      uri_option 'tlsAllowInvalidCertificates', :ssl_verify_certificate,
                 type: :inverse_bool
      uri_option 'tlsAllowInvalidHostnames', :ssl_verify_hostname,
                 type: :inverse_bool
      uri_option 'tlsCAFile', :ssl_ca_cert
      uri_option 'tlsCertificateKeyFile', :ssl_cert
      uri_option 'tlsCertificateKeyFilePassword', :ssl_key_pass_phrase
      uri_option 'tlsInsecure', :ssl_verify, type: :inverse_bool
      uri_option 'tlsDisableOCSPEndpointCheck', :ssl_verify_ocsp_endpoint,
        type: :inverse_bool

      # Topology options
      uri_option 'directConnection', :direct_connection, type: :bool
      uri_option 'connect', :connect, type: :symbol
      uri_option 'loadBalanced', :load_balanced, type: :bool
      uri_option 'srvMaxHosts', :srv_max_hosts, type: :integer
      uri_option 'srvServiceName', :srv_service_name

      # Auth Options
      uri_option 'authSource', :auth_source
      uri_option 'authMechanism', :auth_mech, type: :auth_mech
      uri_option 'authMechanismProperties', :auth_mech_properties, type: :auth_mech_props

      # Client Options
      uri_option 'appName', :app_name
      uri_option 'compressors', :compressors, type: :array
      uri_option 'readConcernLevel', :level, group: :read_concern, type: :symbol
      uri_option 'retryReads', :retry_reads, type: :bool
      uri_option 'retryWrites', :retry_writes, type: :bool
      uri_option 'zlibCompressionLevel', :zlib_compression_level, type: :zlib_compression_level

      # Converts +value+ to a boolean.
      #
      # Returns true for 'true', false for 'false', otherwise nil.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [ String ] URI option value.
      #
      # @return [ true | false | nil ] Converted value.
      def convert_bool(name, value)
        case value
        when "true", 'TRUE'
          true
        when "false", 'FALSE'
          false
        else
          log_warn("invalid boolean option for #{name}: #{value}")
          nil
        end
      end

      def revert_bool(value)
        value
      end

      # Converts the value into a boolean and returns it wrapped in an array.
      #
      # @param name [ String ] Name of the URI option being processed.
      # @param value [ String ] URI option value.
      #
      # @return [ Array<true | false> ] The boolean value parsed and wraped
      #   in an array.
      def convert_repeated_bool(name, value)
        [convert_bool(name, value)]
      end

      def revert_repeated_bool(value)
        value
      end

      # Parses a boolean value and returns its inverse.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [ String ] The URI option value.
      #
      # @return [ true | false | nil ] The inverse of the  boolean value parsed out, otherwise nil
      #   (and a warning will be logged).
      def convert_inverse_bool(name, value)
        b = convert_bool(name, value)

        if b.nil?
          nil
        else
          !b
        end
      end

      def revert_inverse_bool(value)
        !value
      end

      # Converts +value+ into an integer.
      #
      # If the value is not a valid integer, warns and returns nil.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [ String ] URI option value.
      #
      # @return [ nil | Integer ] Converted value.
      def convert_integer(name, value)
        unless /\A\d+\z/ =~ value
          log_warn("#{value} is not a valid integer for #{name}")
          return nil
        end

        value.to_i
      end

      def revert_integer(value)
        value
      end

      # Ruby's convention is to provide timeouts in seconds, not milliseconds and
      # to use fractions where more precision is necessary. The connection string
      # options are always in MS so we provide an easy conversion type.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ Integer ] value The millisecond value.
      #
      # @return [ Float ] The seconds value.
      #
      # @since 2.0.0
      def convert_ms(name, value)
        unless /\A-?\d+(\.\d+)?\z/ =~ value
          log_warn("Invalid ms value for #{name}: #{value}")
          return nil
        end

        if value[0] == '-'
          log_warn("#{name} cannot be a negative number")
          return nil
        end

        value.to_f / 1000
      end

      def revert_ms(value)
        (value * 1000).round
      end

      # Converts +value+ into a symbol.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [ String ] URI option value.
      #
      # @return [ Symbol ] Converted value.
      def convert_symbol(name, value)
        value.to_sym
      end

      def revert_symbol(value)
        value.to_s
      end

      # Extract values from the string and put them into an array.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String ] value The string to build an array from.
      #
      # @return [ Array ] The array built from the string.
      def convert_array(name, value)
        value.split(',')
      end

      def revert_array(value)
        value
      end

      # Authentication mechanism transformation.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [String] The authentication mechanism.
      #
      # @return [Symbol] The transformed authentication mechanism.
      def convert_auth_mech(name, value)
        (AUTH_MECH_MAP[value.upcase] || value).tap do |mech|
          log_warn("#{value} is not a valid auth mechanism") unless mech
        end
      end

      def revert_auth_mech(value)
        found = AUTH_MECH_MAP.detect do |k, v|
          v == value
        end
        if found
          found.first
        else
          raise ArgumentError, "Unknown auth mechanism #{value}"
        end
      end

      # Auth mechanism properties extractor.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [ String ] The auth mechanism properties string.
      #
      # @return [ Hash ] The auth mechanism properties hash.
      def convert_auth_mech_props(name, value)
        properties = hash_extractor('authMechanismProperties', value)
        if properties
          properties.each do |k, v|
            if k.to_s.downcase == 'canonicalize_host_name' && v
              properties[k] = (v.downcase == 'true')
            end
          end
        end
        properties
      end

      def revert_auth_mech_props(value)
        value
      end

      # Parses the max staleness value, which must be either "0" or an integer
      # greater or equal to 90.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [ String ] The max staleness string.
      #
      # @return [ Integer | nil ] The max staleness integer parsed out if it is valid, otherwise nil
      #   (and a warning will be logged).
      def convert_max_staleness(name, value)
        if /\A-?\d+\z/ =~ value
          int = value.to_i

          if int == -1
            int = nil
          end

          if int && (int >= 0 && int < 90 || int < 0)
            log_warn("max staleness should be either 0 or greater than 90: #{value}")
            int = nil
          end

          return int
        end

        log_warn("Invalid max staleness value: #{value}")
        nil
      end

      def revert_max_staleness(value)
        value
      end

      # Read preference mode transformation.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [String] The read mode string value.
      #
      # @return [Symbol] The read mode symbol.
      def convert_read_mode(name, value)
        READ_MODE_MAP[value.downcase] || value
      end

      def revert_read_mode(value)
        value.to_s.gsub(/_(\w)/) { $1.upcase }
      end

      # Read preference tags transformation.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [String] The string representing tag set.
      #
      # @return [Array<Hash>] Array with tag set.
      def convert_read_tags(name, value)
        converted = convert_read_set(name, value)
        if converted
          [converted]
        else
          nil
        end
      end

      def revert_read_tags(value)
        value
      end

      # Read preference tag set extractor.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [String] The tag set string.
      #
      # @return [Hash] The tag set hash.
      def convert_read_set(name, value)
        hash_extractor('readPreferenceTags', value)
      end

      # Converts +value+ as a write concern.
      #
      # If +value+ is the word "majority", returns the symbol :majority.
      # If +value+ is a number, returns the number as an integer.
      # Otherwise returns the string +value+ unchanged.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [ String ] URI option value.
      #
      # @return [ Integer | Symbol | String ] Converted value.
      def convert_w(name, value)
        case value
        when 'majority'
          :majority
        when /\A[0-9]+\z/
          value.to_i
        else
          value
        end
      end

      def revert_w(value)
        case value
        when Symbol
          value.to_s
        else
          value
        end
      end

      # Parses the zlib compression level.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [ String ] The zlib compression level string.
      #
      # @return [ Integer | nil ] The compression level value if it is between -1 and 9 (inclusive),
      #   otherwise nil (and a warning will be logged).
      def convert_zlib_compression_level(name, value)
        if /\A-?\d+\z/ =~ value
          i = value.to_i

          if i >= -1 && i <= 9
            return i
          end
        end

        log_warn("#{value} is not a valid zlibCompressionLevel")
        nil
      end

      def revert_zlib_compression_level(value)
        value
      end

      # Extract values from the string and put them into a nested hash.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param value [ String ] The string to build a hash from.
      #
      # @return [ Hash ] The hash built from the string.
      def hash_extractor(name, value)
        h = {}
        value.split(',').each do |tag|
          k, v = tag.split(':')
          if v.nil?
            log_warn("Invalid hash value for #{name}: key `#{k}` does not have a value: #{value}")
            next
          end

          h[k.to_sym] = v
        end
        if h.empty?
          nil
        else
          h
        end
      end

    end

  end
end
