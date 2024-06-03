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
      # The machine callback workflow is a 1 step execution of the callback
      # to get an OIDC token to connect with.
      class MachineWorkflow
        attr_reader :callback, :callback_lock, :last_executed, :username

        # The number of milliseconds to throttle the callback execution.
        THROTTLE_MS = 100
        # The default timeout for callback execution.
        TIMEOUT_MS = 60000
        # The current OIDC version.
        OIDC_VERSION = 1

        def initialize(auth_mech_properties: {}, username: nil)
          @callback = CallbackFactory.get_callback(auth_mech_properties: auth_mech_properties)
          @callback_lock = Mutex.new
          @username = username
          # Ensure the first execution happens immediately.
          @last_executed = Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond) - THROTTLE_MS - 1
        end

        # Execute the machine callback.
        def execute
          # Aquire lock before executing the callback and throttle calling it
          # to every 100ms.
          callback_lock.synchronize do
            difference = Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond) - last_executed
            if difference <= THROTTLE_MS
              sleep(difference)
            end
            @last_executed = Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond)
            callback.execute(timeout: TIMEOUT_MS, version: OIDC_VERSION, username: username)
          end
        end
      end
    end
  end
end

require 'mongo/auth/oidc/machine_workflow/k8s_callback'
require 'mongo/auth/oidc/machine_workflow/azure_callback'
require 'mongo/auth/oidc/machine_workflow/gcp_callback'
require 'mongo/auth/oidc/machine_workflow/test_callback'
require 'mongo/auth/oidc/machine_workflow/callback_factory'
