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
    extend self

    # Create a read preference object
    #
    # Mongo::NodePreference.get(:secondary, [{'tag' => 'set'}], 20)
    # @todo: acceptable_latency should be grabbed from a global setting (client)
    #
    def get(name = :primary, tag_sets = [], acceptable_latency = 15)
      class_name_str = name.to_s.split('_').each { |s| s.capitalize!}.join
      class_name = Object.const_get("Mongo::NodePreference::#{class_name_str}")
      class_name.new(tag_sets, acceptable_latency)
    end

  end

end

require 'mongo/read_preference/selectable'
require 'mongo/read_preference/nearest'
require 'mongo/read_preference/primary'
require 'mongo/read_preference/primary_preferred'
require 'mongo/read_preference/secondary'
require 'mongo/read_preference/secondary_preferred'
