# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2016-2020 MongoDB Inc.
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

  # This module generates generation numbers from service ids.
  #
  # @api private
  module GenerationGenerator

    # This lock is shared for all servers in the application.
    # This saves resources/memory at the cost of extra contention on JRuby.
    # On MRI there is probably no extra contention.
    LOCK = Mutex.new

    def generation_from_service_id(service_id)
      LOCK.synchronize do
        @service_id_map ||= {}
        @generation ||= 0

        gen = @service_id_map[service_id]
        if gen.nil?
          gen = @service_id_map[service_id] = (@generation += 1)
        end
        gen
      end
    end
  end
end
