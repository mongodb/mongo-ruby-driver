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
        module CallbackFactory
          # Map of environment name to the workflow callbacks.
          CALLBACKS = {
            'k8s' => K8sCallback,
            'azure' => AzureCallback,
            'gcp' => GcpCallback,
            'test' => TestCallback
          }.freeze

          # Gets the callback based on the auth mechanism properties.
          #
          # @params [ Hash ] auth_mech_properties The auth mech properties.
          #
          # @returns [ Callback ] The machine callback.
          module_function def get_callback(auth_mech_properties: {})
            if auth_mech_properties[:oidc_callback]
              auth_mech_properties[:oidc_callback]
            else
              callback = CALLBACKS[auth_mech_properties[:environment]]
              if !callback
                raise Error::OidcError, "No OIDC machine callback found for environment: #{auth_mech_properties[:environment]}"
              end
              callback.new(auth_mech_properties: auth_mech_properties)
            end
          end
        end
      end
    end
  end
end

