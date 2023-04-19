# frozen_string_literal: true
# rubocop:todo all

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
  class Monitoring
    module Event

      # Event fired when a server's description changes.
      #
      # @since 2.4.0
      class ServerDescriptionChanged < Mongo::Event::Base

        # @return [ Address ] address The server address.
        attr_reader :address

        # @return [ Topology ] topology The topology.
        attr_reader :topology

        # @return [ Server::Description ] previous_description The previous server
        #   description.
        attr_reader :previous_description

        # @return [ Server::Description ] new_description The new server
        #   description.
        attr_reader :new_description

        # @return [ true | false ] Whether the heartbeat was awaited.
        #
        # @api experimental
        def awaited?
          @awaited
        end

        # Create the event.
        #
        # @example Create the event.
        #   ServerDescriptionChanged.new(address, topology, previous, new)
        #
        # @param [ Address ] address The server address.
        # @param [ Integer ] topology The topology.
        # @param [ Server::Description ] previous_description The previous description.
        # @param [ Server::Description ] new_description The new description.
        # @param [ true | false ] awaited Whether the server description was
        #   a result of processing an awaited hello response.
        #
        # @since 2.4.0
        # @api private
        def initialize(address, topology, previous_description, new_description,
          awaited: false
        )
          @address = address
          @topology = topology
          @previous_description = previous_description
          @new_description = new_description
          @awaited = !!awaited
        end

        # Returns a concise yet useful summary of the event.
        #
        # @return [ String ] String summary of the event.
        #
        # @note This method is experimental and subject to change.
        #
        # @since 2.7.0
        # @api experimental
        def summary
          "#<#{short_class_name}" +
          " address=#{address}" +
          # TODO Add summaries to descriptions and use them here
          " prev=#{previous_description.server_type.upcase} new=#{new_description.server_type.upcase}#{awaited_indicator}>"
        end

        private

        def awaited_indicator
          if awaited?
            ' [awaited]'
          else
            ''
          end
        end
      end
    end
  end
end
