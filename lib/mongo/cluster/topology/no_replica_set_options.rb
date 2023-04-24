# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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
  class Cluster
    module Topology
      module NoReplicaSetOptions
        private

        def validate_options(options, cluster)
          # These options can be set to nil for convenience, but not to
          # any value including an empty string.
          [:replica_set_name, :max_election_id, :max_set_version].each do |option|
            if options[option]
              raise ArgumentError, "Topology #{self.class.name} cannot have the :#{option} option set"
            end
          end
          super(options, cluster)
        end
      end
    end
  end
end
