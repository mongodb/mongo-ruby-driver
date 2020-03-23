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

        # Redact secure information from the document if it's command is in the
        # list.
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
          if REDACTED_COMMANDS.include?(command_name.to_s) &&
            !%w(1 true yes).include?(ENV['MONGO_RUBY_DRIVER_UNREDACT_EVENTS']&.downcase)
          then
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
