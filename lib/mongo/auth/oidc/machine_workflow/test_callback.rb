# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2024 MongoDB Inc.
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
  module Auth
    class Oidc
      class MachineWorkflow
        class TestCallback
          # We don't need to do anything with the auth mech properties
          # passed in here.
          def initialize(auth_mech_properties: {})
          end

          # Loads the token from the filesystem based on the OIDC_TOKEN_FILE
          # environment variable.
          #
          # @params [ Integer ] timeout The timeout before cancelling.
          # @params [ Integer ] version The OIDC version number.
          # @params [ String ] username The optional username.
          #
          # @returns [ Hash ] The access token.
          def execute(timeout:, version:, username: nil)
            Timeout.timeout(timeout) do
              location = ENV.fetch('OIDC_TOKEN_FILE')
              token = File.read(location)
              { access_token: token }
            end
          end
        end
      end
    end
  end
end
