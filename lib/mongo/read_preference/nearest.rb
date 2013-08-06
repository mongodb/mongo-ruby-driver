# Copyright (C) 2013 10gen Inc.
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
  module ReadPreference
    class Nearest < Mode

      def name
        :nearest
      end

      def to_mongos
        read_preference = { :mode => 'nearest' }
        read_preference.merge!({ :tags => tag_sets }) unless tag_sets.empty?
        read_preference
      end

      def select_nodes(candidates)
        if !tag_sets.empty?
          near(match_tag_set(candidates))
        else
          near(candidates)
        end
      end
    end
  end
end
