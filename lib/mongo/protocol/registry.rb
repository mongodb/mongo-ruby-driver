# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2009-2020 MongoDB Inc.
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

  module Protocol

    # Provides a registry for looking up a message class based on op code.
    #
    # @since 2.5.0
    module Registry
      extend self

      # A Mapping of all the op codes to their corresponding Ruby classes.
      #
      # @since 2.5.0
      MAPPINGS = {}

      # Get the class for the given op code and raise an error if it's not found.
      #
      # @example Get the type for the op code.
      #   Mongo::Protocol::Registry.get(1)
      #
      # @return [ Class ] The corresponding Ruby class for the message type.
      #
      # @since 2.5.0
      def get(op_code, message = nil)
        if type = MAPPINGS[op_code]
          type
        else
          handle_unsupported_op_code!(op_code)
        end
      end

      # Register the Ruby type for the corresponding op code.
      #
      # @example Register the op code.
      #   Mongo::Protocol::Registry.register(1, Reply)
      #
      # @param [ Fixnum ] op_code The op code.
      # @param [ Class ] type The class the op code maps to.
      #
      # @return [ Class ] The class.
      #
      # @since 2.5.0
      def register(op_code, type)
        MAPPINGS.store(op_code, type)
        define_type_reader(type)
      end

      private

      def define_type_reader(type)
        type.module_eval <<-MOD
          def op_code; OP_CODE; end
        MOD
      end

      def handle_unsupported_op_code!(op_code)
        message = "Detected unknown message type with op code: #{op_code}."
        raise Error::UnsupportedMessageType.new(message)
      end
    end
  end
end
