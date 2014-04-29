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

require 'mongo/node_preference/selectable'
require 'mongo/node_preference/nearest'
require 'mongo/node_preference/primary'
require 'mongo/node_preference/primary_preferred'
require 'mongo/node_preference/secondary'
require 'mongo/node_preference/secondary_preferred'

module Mongo

  # Functionality for getting an object representing a specific node preference.
  #
  # @since 3.0.0
  module NodePreference
    extend self

    # Create a node preference object.
    #
    # @example Get a node preference object for selecting a secondary with
    #   specific tag sets and acceptable latency.
    #   Mongo::NodePreference.get(:secondary, [{'tag' => 'set'}], 20)
    #
    #  @param [ Symbol ] mode The name of the node preference mode.
    #  @param [ Array ] tag_sets The tag sets to be used when selecting nodes.
    #  @param [ Integer ] acceptable_latency (15) The acceptable latency in milliseconds
    #    to be used when selecting nodes.
    #
    # @since 3.0.0
    # @todo: acceptable_latency should be grabbed from a global setting (client)
    def get(mode = :primary, tag_sets = [], acceptable_latency = 15)
      class_name_str = mode.to_s.split('_').each { |s| s.capitalize!}.join
      class_name = Object.const_get("Mongo::NodePreference::#{class_name_str}")
      class_name.new(tag_sets, acceptable_latency)
    end
  end
end

