# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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
  class Monitoring
    module Event

      # Provides behavior to redact sensitive information from commands and
      # replies.
      #
      # @since 2.1.0
      module Secure

        # The list of commands that has the data redacted for security.
        #
        # @since 2.1.0
        REDACTED_COMMANDS = [
          'authenticate',
          'saslStart',
          'saslContinue',
          'getnonce',
          'createUser',
          'updateUser',
          'copydbgetnonce',
          'copydbsaslstart',
          'copydb'
        ].freeze

        # Check whether the command is sensitive in terms of command monitoring
        # spec. A command is detected as sensitive if it is in the
        # list or if it is a hello/legacy hello command, and
        # speculative authentication is enabled.
        #
        # @param [ String, Symbol ] command_name The command name.
        # @param [ BSON::Document ] document The document.
        #
        # @return [ true | false ] Whether the command is sensitive.
        def sensitive?(command_name:, document:)
          if REDACTED_COMMANDS.include?(command_name.to_s)
            true
          elsif %w(hello ismaster isMaster).include?(command_name.to_s) &&
            document['speculativeAuthenticate']
            then
            # According to Command Monitoring spec,for hello/legacy hello commands
            # when speculativeAuthenticate is present, their commands AND replies
            # MUST be redacted from the events.
            # See https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst#security
            true
          else
            false
          end
        end

        # Redact secure information from the document if:
        #   - its command is in the sensitive commands;
        #   - its command is a hello/legacy hello command, and
        #     speculative authentication is enabled;
        #   - corresponding started event is sensitive.
        #
        # @example Get the redacted document.
        #   secure.redacted(command_name, document)
        #
        # @param [ String, Symbol ] command_name The command name.
        # @param [ BSON::Document ] document The document.
        #
        # @return [ BSON::Document ] The redacted document.
        #
        # @since 2.1.0
        def redacted(command_name, document)
          if %w(1 true yes).include?(ENV['MONGO_RUBY_DRIVER_UNREDACT_EVENTS']&.downcase)
            document
          elsif respond_to?(:started_event) && started_event.sensitive
            return BSON::Document.new
          elsif sensitive?(command_name: command_name, document: document)
            BSON::Document.new
          else
            document
          end
        end


        # Is compression allowed for a given command message.
        #
        # @example Determine if compression is allowed for a given command.
        #   secure.compression_allowed?(selector)
        #
        # @param [ String, Symbol ] command_name The command name.
        #
        # @return [ true, false ] Whether compression can be used.
        #
        # @since 2.5.0
        def compression_allowed?(command_name)
          @compression_allowed ||= !REDACTED_COMMANDS.include?(command_name.to_s)
        end
      end
    end
  end
end
