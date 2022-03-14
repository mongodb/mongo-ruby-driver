# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2021 MongoDB Inc.
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
  class Server
    class ConnectionPool

      # @api private
      class GenerationManager

        def initialize(server:)
          @map = Hash.new { |hash, key| hash[key] = 1 }
          @server = server
          @lock = Mutex.new
        end

        attr_reader :server

        def generation(connection_global_id: nil)
          key = if server.load_balancer?
            if connection_global_id.nil?
              raise ArgumentError, "The server at #{server.address} is a load balancer and therefore does not have a single global generation"
            end
            connection_global_id
          else
            nil
          end
          @lock.synchronize do
            @map[key]
          end
        end

        def bump(connection_global_id: nil)
          @lock.synchronize do
            if server.load_balancer? && connection_global_id
              @map[connection_global_id] += 1
            else
              # When connection id is not supplied, one of two things may be
              # happening;
              #
              # 1. The pool is not to a load balancer, in which case we only
              #    need to increment the generation for the nil connection_global_id.
              # 2. The pool is to a load balancer, in which case we need to
              #    increment the generation for each connection.
              #
              # Incrementing everything in the map accomplishes both tasks.
              @map.each do |k, v|
                @map[k] += 1
              end
            end
          end
        end
      end
    end
  end
end
