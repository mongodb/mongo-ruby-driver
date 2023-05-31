# frozen_string_literal: true

# Copyright (C) 2016-2020 MongoDB Inc.
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

require 'mongo/server/app_metadata/environment'

module Mongo
  class Server
    class AppMetadata
      # @api private
      class Truncator
        attr_reader :document

        # The max application metadata document byte size.
        MAX_DOCUMENT_SIZE = 512

        def initialize(document)
          @document = document
          try_truncate!
        end

        def size
          @document.to_bson.to_s.length
        end

        def ok?
          size <= MAX_DOCUMENT_SIZE
        end

        private

        def excess
          size - MAX_DOCUMENT_SIZE
        end

        def try_truncate!
          %i[ platform env os env_name os_type driver app_name ].each do |target|
            break if ok?
            send(:"try_truncate_#{target}!")
          end
        end

        def try_truncate_platform!
          unless try_truncate_string(@document[:platform])
            @document.delete(:platform)
          end
        end

        def try_truncate_env!
          try_truncate_hash(@document[:env], reserved: %w[ name ])
        end

        def try_truncate_os!
          try_truncate_hash(@document[:os], reserved: %w[ type ])
        end

        def try_truncate_env_name!
          return unless @document[:env]

          unless try_truncate_string(@document[:env][:name])
            @document.delete(:env)
          end
        end

        def try_truncate_os_type!
          return unless @document[:os]

          unless try_truncate_string(@document[:os][:type])
            @document.delete(:os)
          end
        end

        def try_truncate_driver!
          unless try_truncate_hash(@document[:driver])
            @document.delete(:driver)
          end
        end

        def try_truncate_app_name!
          unless try_truncate_string(@document[:application][:name])
            @document.delete(:application)
          end
        end

        def try_truncate_string(string)
          length = string&.length || 0

          return false if excess > length

          string[(length - excess)..] = ""
        end

        def try_truncate_hash(hash, reserved: [])
          return false unless hash

          keys = hash.keys - reserved
          keys.each do |key|
            unless try_truncate_string(hash[key].to_s)
              hash.delete(key)
            end

            return true if ok?
          end

          false
        end
      end
    end
  end
end
