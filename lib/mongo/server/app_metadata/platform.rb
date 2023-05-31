# frozen_string_literal: true

# Copyright (C) 2016-2023 MongoDB Inc.
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
  class Server
    class AppMetadata
      # Implements the logic for building the platform string for the
      # handshake.
      #
      # @api private
      class Platform
        # @return [ Mongo::Server::AppMetadata ] the metadata object to
        #   reference when building the platform string.
        attr_reader :metadata

        # Create a new Platform object, referencing the given metadata object.
        #
        # @param [ Mongo::Server::AppMetadata ] metadata the metadata object
        #   the reference when building the platform string.
        def initialize(metadata)
          @metadata = metadata
        end

        # Queries whether the current runtime is JRuby or not.
        #
        # @return [ true | false ] whether the runtime is JRuby or not.
        def jruby?
          BSON::Environment.jruby?
        end

        # Returns the list of Ruby versions that identify this runtime.
        #
        # @return [ Array<String> ] the list of ruby versions
        def ruby_versions
          if jruby?
            [ "JRuby #{JRUBY_VERSION}", "like Ruby #{RUBY_VERSION}" ]
          else
            [ "Ruby #{RUBY_VERSION}" ]
          end
        end

        # Returns the list of platform identifiers that identify this runtime.
        #
        # @return [ Array<String> ] the list of platform identifiers.
        def platforms
          [ RUBY_PLATFORM ].tap do |list|
            list.push "JVM #{java_version}" if jruby?
          end
        end

        # Returns the version of the current Java environment, or nil if not
        # invoked with JRuby.
        #
        # @return [ String | nil ] the current Java version
        def java_version
          return nil unless jruby?

          java.lang.System.get_property('java.version')
        end

        # Builds and returns the default platform list, for use when building
        # the platform string.
        #
        # @return [ Array<String> ] the list of platform identifiers
        def default_platform_list
          [
            metadata.platform,
            *ruby_versions,
            *platforms,
            RbConfig::CONFIG['build']
          ]
        end

        # Returns a single letter representing the purpose reported to the
        # metadata, or nil if no purpose was specified.
        #
        # @return [ String | nil ] the code representing the purpose
        def purpose
          return nil unless metadata.purpose

          metadata.purpose.to_s[0].upcase
        end

        # Builds and returns the platform string by concatenating relevant
        # values together.
        #
        # @return [ String ] the platform string
        def to_s
          primary = [ *default_platform_list, purpose ].compact.join(', ')
          list = [ primary ]

          metadata.wrapping_libraries&.each do |library|
            list << (library[:platform] || '')
          end

          list.join('|')
        end
      end
    end
  end
end
