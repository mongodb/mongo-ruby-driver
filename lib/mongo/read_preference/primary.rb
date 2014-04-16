# Copyright (C) 2009-2014 MongoDB, Inc.
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

  module NodePreference

    # Behavior for a primary read preference.
    class Primary
      include Selectable

      def name
        :primary
      end

      def slave_ok?
        false
      end

      def tags_allowed?
        false
      end

      def to_mongos
        nil
      end

      def select_nodes(candidates)
        primary(candidates)
      end
    end

  end

end