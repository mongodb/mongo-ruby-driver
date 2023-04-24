# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Server
    class Monitor
      # App metadata for monitoring sockets.
      #
      # It is easiest to start with the normal app metadata and remove
      # authentication-related bits.
      #
      # @api private
      class AppMetadata < Server::AppMetadata
        def initialize(options = {})
          super
          if instance_variable_defined?(:@request_auth_mech)
            remove_instance_variable(:@request_auth_mech)
          end
        end
      end
    end
  end
end
