# frozen_string_literal: true

# Copyright (C) 2015-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Options
    # Shared string, debug and serialization representations for option
    # objects that may carry sensitive values.
    #
    # Sensitive values are replaced by {Redacted::STRING_REPLACEMENT} in
    # inspect, to_s, pretty printing, JSON, YAML, Extended JSON and Marshal
    # output. The stored values are not changed.
    #
    # BSON encoding is intentionally not overridden: these objects are BSON
    # documents, and BSON is the wire representation used by the driver. The
    # native BSON encoder writes nested hashes without dispatching to Ruby
    # methods on them, so a Ruby-level to_bson override would be both
    # incomplete and would change what is written to the wire.
    #
    # @api private
    module RedactedRepresentation
      # Envelope keys used by YAML and Marshal. They surround, rather than
      # live inside, the user's options hash.
      # @api private
      SERIALIZATION_REDACTED_KEY = 'redacted'

      # @api private
      SERIALIZATION_OPTIONS_KEY = 'options'

      # Error raised when deserializing a representation whose sensitive
      # values were redacted.
      REDACTED_SERIALIZATION_ERROR =
        'Cannot deserialize options whose sensitive values were redacted: ' \
        'recreate the options from configuration instead.'

      # Error raised for a payload that reaches the new object-tag YAML
      # loader or the user-defined Marshal loader but does not contain the
      # redaction envelope. Legacy Hash formats (for example YAML with a
      # !ruby/hash tag, or standard Marshal data for a Hash subclass) can
      # bypass those hooks and are not covered by this error.
      LEGACY_SERIALIZATION_ERROR =
        'Cannot deserialize options without a redaction envelope: ' \
        'recreate the options from configuration instead.'

      # Get a string representation of the options.
      #
      # @return [ String ] The string representation of the options.
      def inspect
        redacted_string(:inspect)
      end

      # Get a string representation of the options.
      #
      # @return [ String ] The string representation of the options.
      def to_s
        redacted_string(:to_s)
      end

      # Pretty print the options, redacting sensitive values.
      #
      # @param [ PP ] q The pretty printer.
      def pretty_print(q)
        q.pp(redacted_serialization_hash)
      end

      # Serialize the options to JSON, redacting sensitive values.
      #
      # @param [ Array ] args JSON generator arguments and state.
      #
      # @return [ String ] The JSON representation.
      def to_json(*args)
        redacted_serialization_hash.to_json(*args)
      end

      # Serialize the options to YAML, redacting sensitive values.
      #
      # The user's options are nested under an envelope so that metadata
      # cannot collide with option names. Deserialization raises when
      # sensitive values were redacted.
      #
      # @param [ Psych::Coder ] coder The YAML coder.
      def encode_with(coder)
        coder.tag = "!ruby/object:#{self.class.name}"
        serialization_envelope.each do |key, value|
          coder[key] = value
        end
      end

      # Initialize options from YAML.
      #
      # Raises if the serialized form contains redacted sensitive values or
      # does not use the redaction envelope. It never creates an object with
      # marker strings standing in for credentials.
      #
      # @param [ Psych::Coder ] coder The YAML coder.
      def init_with(coder)
        load_serialization_envelope(coder.map)
      end

      # Serialize the options to Extended JSON, redacting sensitive values.
      #
      # @param [ Hash ] options BSON Extended JSON options.
      #
      # @return [ String ] The Extended JSON representation.
      def to_extended_json(**options)
        redacted_serialization_hash.to_extended_json(**options)
      end

      # Convert the options to a Hash for BSON Extended JSON encoding.
      #
      # @param [ Hash ] options BSON Extended JSON options.
      #
      # @return [ Hash ] The redacted Hash.
      def as_extended_json(**options)
        redacted_serialization_hash.as_extended_json(**options)
      end

      # Convert the options to a Hash for ActiveSupport's JSON encoding.
      #
      # @param [ Hash | nil ] options The ActiveSupport options.
      #
      # @return [ Hash ] The redacted Hash.
      def as_json(options = nil)
        redacted = redacted_serialization_hash
        if redacted.respond_to?(:as_json)
          redacted.as_json(options)
        else
          redacted
        end
      end

      # Dump the options for Marshal, redacting sensitive values.
      #
      # @return [ Hash ] The envelope with redacted options.
      def marshal_dump
        serialization_envelope
      end

      # Load the options from Marshal.
      #
      # Raises if the serialized form contains redacted sensitive values or
      # does not use the redaction envelope. It never creates an object with
      # marker strings standing in for credentials.
      #
      # @param [ Hash ] hash The dumped envelope.
      def marshal_load(hash)
        load_serialization_envelope(hash)
      end

      private

      def redacted_string(method)
        '{' + map do |key, value|
          "#{key.public_send(method)}=>#{redacted_value(key, value, method)}"
        end.join(', ') + '}'
      end

      def redacted_value(key, value, method)
        return Redacted::STRING_REPLACEMENT if sensitive_key?(key)

        value.public_send(method)
      end

      def redacted_serialization_hash
        each_with_object({}) do |(key, value), copy|
          copy[key] = sensitive_key?(key) ? Redacted::STRING_REPLACEMENT : value
        end
      end

      def sensitive_key?(key)
        return false unless key.respond_to?(:to_sym)

        Redacted::SENSITIVE_OPTIONS.include?(key.to_sym)
      end

      def has_sensitive_keys?
        keys.any? { |key| sensitive_key?(key) }
      end

      def serialization_envelope
        {
          SERIALIZATION_REDACTED_KEY => has_sensitive_keys?,
          SERIALIZATION_OPTIONS_KEY => redacted_serialization_hash,
        }
      end

      def load_serialization_envelope(envelope)
        unless envelope.is_a?(Hash) &&
               envelope.key?(SERIALIZATION_REDACTED_KEY) &&
               envelope.key?(SERIALIZATION_OPTIONS_KEY)
          raise ArgumentError, LEGACY_SERIALIZATION_ERROR
        end

        options = envelope[SERIALIZATION_OPTIONS_KEY] || {}
        if envelope[SERIALIZATION_REDACTED_KEY] ||
           options.keys.any? { |key| sensitive_key?(key) }
          raise ArgumentError, REDACTED_SERIALIZATION_ERROR
        end

        options.each do |key, value|
          self[key] = value
        end
      end
    end

    # Class for wrapping options that could be sensitive.
    #
    # Sensitive values are redacted in inspect, to_s, pretty printed, JSON,
    # YAML and Marshal output. The stored values are unchanged. Deserializing
    # a YAML or Marshal representation that contained sensitive values raises
    # ArgumentError instead of creating an object with marker credentials.
    #
    # @since 2.1.0
    class Redacted < BSON::Document
      include RedactedRepresentation

      # The options whose values will be redacted. The username is included
      # because it is part of the userinfo segment, whose components the
      # connection string specification treats as sensitive values.
      #
      # The arguments of the sensitive URI options are redacted as a whole:
      # authMechanismProperties may carry a credential in any of its
      # properties (for example an AWS session token), and the driver cannot
      # know which properties a future mechanism will treat as secret.
      #
      # @since 2.1.0
      SENSITIVE_OPTIONS = %i[user
                             password
                             pwd
                             proxy_password
                             proxy_username
                             ssl_key_pass_phrase
                             auth_mech_properties].freeze

      # The replacement string used in place of the value for sensitive keys.
      #
      # @since 2.1.0
      STRING_REPLACEMENT = '<REDACTED>'

      # Whether these options contain a given key.
      #
      # @example Determine if the options contain a given key.
      #   options.has_key?(:name)
      #
      # @param [ String, Symbol ] key The key to check for existence.
      #
      # @return [ true, false ] If the options contain the given key.
      #
      # @since 2.1.0
      def has_key?(key)
        super(convert_key(key))
      end
      alias key? has_key?

      # Returns a new options object consisting of pairs for which the block returns false.
      #
      # @example Get a new options object with pairs for which the block returns false.
      #   new_options = options.reject { |k, v| k == 'database' }
      #
      # @yieldparam [ String, Object ] The key as a string and its value.
      #
      # @return [ Options::Redacted ] A new options object.
      #
      # @since 2.1.0
      def reject(&block)
        new_options = dup
        new_options.reject!(&block) || new_options
      end

      # Only keeps pairs for which the block returns false.
      #
      # @example Remove pairs from this object for which the block returns true.
      #   options.reject! { |k, v| k == 'database' }
      #
      # @yieldparam [ String, Object ] The key as a string and its value.
      #
      # @return [ Options::Redacted, nil ] This object or nil if no changes were made.
      #
      # @since 2.1.0
      def reject!
        if block_given?
          n_keys = keys.size
          keys.each do |key|
            delete(key) if yield(key, self[key])
          end
          (n_keys == keys.size) ? nil : self
        else
          to_enum
        end
      end

      # Returns a new options object consisting of pairs for which the block returns true.
      #
      # @example Get a new options object with pairs for which the block returns true.
      #   ssl_options = options.select { |k, v| k =~ /ssl/ }
      #
      # @yieldparam [ String, Object ] The key as a string and its value.
      #
      # @return [ Options::Redacted ] A new options object.
      #
      # @since 2.1.0
      def select(&block)
        new_options = dup
        new_options.select!(&block) || new_options
      end

      # Only keeps pairs for which the block returns true.
      #
      # @example Remove pairs from this object for which the block does not return true.
      #   options.select! { |k, v| k =~ /ssl/ }
      #
      # @yieldparam [ String, Object ] The key as a string and its value.
      #
      # @return [ Options::Redacted, nil ] This object or nil if no changes were made.
      #
      # @since 2.1.0
      def select!
        if block_given?
          n_keys = keys.size
          keys.each do |key|
            delete(key) unless yield(key, self[key])
          end
          (n_keys == keys.size) ? nil : self
        else
          to_enum
        end
      end
    end

    # A plain Hash whose string and serialized representations redact
    # sensitive values.
    #
    # Unlike {Redacted}, which is a BSON::Document and normalizes its keys to
    # strings, this class preserves the keys exactly as they were inserted.
    # It is used for option hashes whose key types are part of the driver's
    # observable behavior. Accessing the hash returns the real values;
    # deserializing a representation with sensitive values raises
    # ArgumentError.
    #
    # @api private
    class RedactedHash < Hash
      include RedactedRepresentation
    end
  end
end
