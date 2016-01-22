# Copyright (C) 2016 MongoDB, Inc.
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

      # @since 2.3.0
      class ServerDescriptionChanged

        attr_reader :address

        attr_reader :cluster_id

        attr_reader :old_description

        attr_reader :new_description

        def initialize(address, cluster_id, old_description, new_description)
          @address = address
          @cluster_id = cluster_id
          @old_description = old_description
          @new_description = new_description
        end
      end
    end
  end
end

