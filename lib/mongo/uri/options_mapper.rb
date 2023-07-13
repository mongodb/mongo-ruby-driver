# frozen_string_literal: true
# rubocop:todo all

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
            if type = spec[:type]
              v = send("revert_#{type}", v)
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

      # Converts Ruby options provided to their representation in a URI string.
      #
      # @param [ Hash ] opts Ruby options to convert.
      #
      # @return [ Hash ] URI string hash.
      def ruby_to_string(opts)
        rv = {}
        URI_OPTION_MAP.each do |uri_key, spec|
          if spec[:group]
            v = opts[spec[:group]]
            v = v && v[spec[:name]]
          else
            v = opts[spec[:name]]
          end
          unless v.nil?
            if type = spec[:type]
              v = send("stringify_#{type}", v)
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
      # @param [ String ] key URI option name.
      # @param [ String ] value The value to be transformed.
      # @param [ Symbol ] type The transform method.
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
      # @param [ Hash ] target The destination.
      # @param [ Object ] value The value to be merged.
      # @param [ Symbol ] name The name of the option.
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
      # @param [ String ] uri_key The MongoDB URI option to register.
      # @param [ Symbol ] name The name of the option in the driver.
      # @param [ Hash ] extra Extra options.
      #   * :group [ Symbol ] Nested hash where option will go.
      #   * :type [ Symbol ] Name of function to transform value.
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
      uri_option 'maxConnecting', :max_connecting, type: :integer
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
      # @param [ String | true | false ] value URI option value.
      #
      # @return [ true | false | nil ] Converted value.
      def convert_bool(name, value)
        case value
        when true, "true", 'TRUE'
          true
        when false, "false", 'FALSE'
          false
        else
          log_warn("invalid boolean option for #{name}: #{value}")
          nil
        end
      end

      # Reverts a boolean type.
      #
      # @param [ true | false | nil ] value The boolean to revert.
      #
      # @return [ true | false | nil ] The passed value.
      def revert_bool(value)
        value
      end

      # Stringifies a boolean type.
      #
      # @param [ true | false | nil ] value The boolean.
      #
      # @return [ String | nil ] The string.
      def stringify_bool(value)
        revert_bool(value)&.to_s
      end

      # Converts the value into a boolean and returns it wrapped in an array.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String ] value URI option value.
      #
      # @return [ Array<true | false> | nil ] The boolean value parsed and wraped
      #   in an array.
      def convert_repeated_bool(name, value)
        [convert_bool(name, value)]
      end

      # Reverts a repeated boolean type.
      #
      # @param [ Array<true | false> | true | false | nil ] value The repeated boolean to revert.
      #
      # @return [ Array<true | false> | true | false | nil ] The passed value.
      def revert_repeated_bool(value)
        value
      end

      # Stringifies a repeated boolean type.
      #
      # @param [ Array<true | false> | nil ] value The repeated boolean.
      #
      # @return [ Array<true | false> | nil ] The string.
      def stringify_repeated_bool(value)
        rep = revert_repeated_bool(value)
        if rep&.is_a?(Array)
          rep.join(",")
        else
          rep
        end
      end

      # Parses a boolean value and returns its inverse.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String | true | false ] value The URI option value.
      #
      # @return [ true | false | nil ] The inverse of the boolean value parsed out, otherwise nil
      #   (and a warning will be logged).
      def convert_inverse_bool(name, value)
        b = convert_bool(name, value)

        if b.nil?
          nil
        else
          !b
        end
      end

      # Reverts and inverts a boolean type.
      #
      # @param [ true | false | nil ] value The boolean to revert and invert.
      #
      # @return [ true | false | nil ] The inverted boolean.
      def revert_inverse_bool(value)
        value.nil? ? nil : !value
      end

      # Inverts and stringifies a boolean.
      #
      # @param [ true | false | nil ] value The boolean.
      #
      # @return [ String | nil ] The string.
      def stringify_inverse_bool(value)
        revert_inverse_bool(value)&.to_s
      end

      # Converts +value+ into an integer. Only converts positive integers.
      #
      # If the value is not a valid integer, warns and returns nil.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String | Integer ] value URI option value.
      #
      # @return [ nil | Integer ] Converted value.
      def convert_integer(name, value)
        if value.is_a?(String) && /\A\d+\z/ !~ value
          log_warn("#{value} is not a valid integer for #{name}")
          return nil
        end

        value.to_i
      end

      # Reverts an integer.
      #
      # @param [ Integer | nil ] value The integer.
      #
      # @return [ Integer | nil ] The passed value.
      def revert_integer(value)
        value
      end

      # Stringifies an integer.
      #
      # @param [ Integer | nil ] value The integer.
      #
      # @return [ String | nil ] The string.
      def stringify_integer(value)
        revert_integer(value)&.to_s
      end

      # Ruby's convention is to provide timeouts in seconds, not milliseconds and
      # to use fractions where more precision is necessary. The connection string
      # options are always in MS so we provide an easy conversion type.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String | Integer | Float ] value The millisecond value.
      #
      # @return [ Float ] The seconds value.
      #
      # @since 2.0.0
      def convert_ms(name, value)
        case value
        when String
          if /\A-?\d+(\.\d+)?\z/ !~ value
            log_warn("Invalid ms value for #{name}: #{value}")
            return nil
          end
          if value.to_s[0] == '-'
            log_warn("#{name} cannot be a negative number")
            return nil
          end
        when Integer, Float
          if value < 0
            log_warn("#{name} cannot be a negative number")
            return nil
          end
        else
          raise ArgumentError, "Can only convert Strings, Integers, or Floats to ms. Given: #{value.class}"
        end

        value.to_f / 1000
      end

      # Reverts an ms.
      #
      # @param [ Float ] value The float.
      #
      # @return [ Integer ] The number multiplied by 1000 as an integer.
      def revert_ms(value)
        (value * 1000).round
      end

      # Stringifies an ms.
      #
      # @param [ Float ] value The float.
      #
      # @return [ String ] The string.
      def stringify_ms(value)
        revert_ms(value).to_s
      end

      # Converts +value+ into a symbol.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String | Symbol ] value URI option value.
      #
      # @return [ Symbol ] Converted value.
      def convert_symbol(name, value)
        value.to_sym
      end

      # Reverts a symbol.
      #
      # @param [ Symbol ] value The symbol.
      #
      # @return [ String ] The passed value as a string.
      def revert_symbol(value)
        value.to_s
      end
      alias :stringify_symbol :revert_symbol

      # Extract values from the string and put them into an array.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String ] value The string to build an array from.
      #
      # @return [ Array<String> ] The array built from the string.
      def convert_array(name, value)
        value.split(',')
      end

      # Reverts an array.
      #
      # @param [ Array<String> ] value An array of strings.
      #
      # @return [ Array<String> ] The passed value.
      def revert_array(value)
        value
      end

      # Stringifies an array.
      #
      # @param [ Array<String> ] value An array of strings.
      #
      # @return [ String ] The array joined by commas.
      def stringify_array(value)
        value.join(',')
      end

      # Authentication mechanism transformation.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String ] value The authentication mechanism.
      #
      # @return [ Symbol ] The transformed authentication mechanism.
      def convert_auth_mech(name, value)
        auth_mech = AUTH_MECH_MAP[value.upcase]
        (auth_mech || value).tap do |mech|
          log_warn("#{value} is not a valid auth mechanism") unless auth_mech
        end
      end

      # Reverts auth mechanism.
      #
      # @param [ Symbol ] value The auth mechanism.
      #
      # @return [ String ] The auth mechanism as a string.
      #
      # @raise [ ArgumentError ] if its an invalid auth mechanism.
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

      # Stringifies auth mechanism.
      #
      # @param [ Symbol ] value The auth mechanism.
      #
      # @return [ String | nil ] The auth mechanism as a string.
      def stringify_auth_mech(value)
        revert_auth_mech(value) rescue nil
      end

      # Auth mechanism properties extractor.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String ] value The auth mechanism properties string.
      #
      # @return [ Hash | nil ] The auth mechanism properties hash.
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

      # Reverts auth mechanism properties.
      #
      # @param [ Hash | nil ] value The auth mech properties.
      #
      # @return [ Hash | nil ] The passed value.
      def revert_auth_mech_props(value)
        value
      end

      # Stringifies auth mechanism properties.
      #
      # @param [ Hash | nil ] value The auth mech properties.
      #
      # @return [ String | nil ] The string.
      def stringify_auth_mech_props(value)
        return if value.nil?
        value.map { |k, v| "#{k}:#{v}" }.join(',')
      end

      # Parses the max staleness value, which must be either "0" or an integer
      # greater or equal to 90.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String | Integer ] value The max staleness string.
      #
      # @return [ Integer | nil ] The max staleness integer parsed out if it is valid, otherwise nil
      #   (and a warning will be logged).
      def convert_max_staleness(name, value)
        int = if value.is_a?(String) && /\A-?\d+\z/ =~ value
          value.to_i
        elsif value.is_a?(Integer)
          value
        end

        if int.nil?
          log_warn("Invalid max staleness value: #{value}")
          return nil
        end

        if int == -1
          int = nil
        end

        if int && (int > 0 && int < 90 || int < 0)
          log_warn("max staleness should be either 0 or greater than 90: #{value}")
          int = nil
        end

        int
      end

      # Reverts max staleness.
      #
      # @param [ Integer | nil ] value The max staleness.
      #
      # @return [ Integer | nil ] The passed value.
      def revert_max_staleness(value)
        value
      end

      # Stringifies max staleness.
      #
      # @param [ Integer | nil ] value The max staleness.
      #
      # @return [ String | nil ] The string.
      def stringify_max_staleness(value)
        revert_max_staleness(value)&.to_s
      end

      # Read preference mode transformation.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String ] value The read mode string value.
      #
      # @return [ Symbol | String ] The read mode.
      def convert_read_mode(name, value)
        READ_MODE_MAP[value.downcase] || value
      end

      # Reverts read mode.
      #
      # @param [ Symbol | String ] value The read mode.
      #
      # @return [ String ] The read mode as a string.
      def revert_read_mode(value)
        value.to_s.gsub(/_(\w)/) { $1.upcase }
      end
      alias :stringify_read_mode :revert_read_mode

      # Read preference tags transformation.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String ] value The string representing tag set.
      #
      # @return [ Array<Hash> | nil ] Array with tag set.
      def convert_read_tags(name, value)
        converted = convert_read_set(name, value)
        if converted
          [converted]
        else
          nil
        end
      end

      # Reverts read tags.
      #
      # @param [ Array<Hash> | nil ] value The read tags.
      #
      # @return [ Array<Hash> | nil ] The passed value.
      def revert_read_tags(value)
        value
      end

      # Stringifies read tags.
      #
      # @param [ Array<Hash> | nil ] value The read tags.
      #
      # @return [ String | nil ] The joined string of read tags.
      def stringify_read_tags(value)
        value&.map { |ar| ar.map { |k, v| "#{k}:#{v}" }.join(',') }
      end

      # Read preference tag set extractor.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String ] value The tag set string.
      #
      # @return [ Hash ] The tag set hash.
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
      # @param [ String | Integer ] value URI option value.
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

      # Reverts write concern.
      #
      # @param [ Integer | Symbol | String ] value The write concern.
      #
      # @return [ Integer | String ] The write concern as a string.
      def revert_w(value)
        case value
        when Symbol
          value.to_s
        else
          value
        end
      end

      # Stringifies write concern.
      #
      # @param [ Integer | Symbol | String ] value The write concern.
      #
      # @return [ String ] The write concern as a string.
      def stringify_w(value)
        revert_w(value)&.to_s
      end

      # Parses the zlib compression level.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String | Integer ] value The zlib compression level string.
      #
      # @return [ Integer | nil ] The compression level value if it is between -1 and 9 (inclusive),
      #   otherwise nil (and a warning will be logged).
      def convert_zlib_compression_level(name, value)
        i = if value.is_a?(String) && /\A-?\d+\z/ =~ value
          value.to_i
        elsif value.is_a?(Integer)
          value
        end

        if i && (i >= -1 && i <= 9)
          i
        else
          log_warn("#{value} is not a valid zlibCompressionLevel")
          nil
        end
      end

      # Reverts zlib compression level
      #
      # @param [ Integer | nil ] value The write concern.
      #
      # @return [ Integer | nil ] The passed value.
      def revert_zlib_compression_level(value)
        value
      end

      # Stringifies zlib compression level
      #
      # @param [ Integer | nil ] value The write concern.
      #
      # @return [ String | nil ] The string.
      def stringify_zlib_compression_level(value)
        revert_zlib_compression_level(value)&.to_s
      end

      # Extract values from the string and put them into a nested hash.
      #
      # @param [ String ] name Name of the URI option being processed.
      # @param [ String ] value The string to build a hash from.
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
