# Copyright (C) 2014-2019 MongoDB, Inc.
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
  class Error

    # Raised if there are no servers available matching the preference.
    #
    # @since 2.0.0
    class NoServerAvailable < Error

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::NoServerAvailable.new(server_selector)
      #
      # @param [ Hash ] server_selector The server preference that could not be
      #   satisfied.
      # @param [ Cluster ] cluster The cluster that server selection was
      #   performed on. (added in 2.7.0)
      #
      # @since 2.0.0
      def initialize(server_selector, cluster=nil, msg=nil)
        msg ||= "No #{server_selector.name} server is available in cluster: #{cluster.summary} " +
                "with timeout=#{server_selector.server_selection_timeout}, " +
                "LT=#{server_selector.local_threshold}"

        super(msg)
      end
    end
  end
end
