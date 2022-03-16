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

        def generation(service_id: nil)
          if service_id
            unless server.load_balancer?
              raise ArgumentError, "Generation scoping to services is only available in load-balanced mode, but the server at #{server.address} is not a load balancer"
            end
          else
            if server.load_balancer?
              raise ArgumentError, "The server at #{server.address} is a load balancer and therefore does not have a single global generation"
            end
          end
          @lock.synchronize do
            @map[service_id]
          end
        end

        def bump(service_id: nil)
          @lock.synchronize do
            if service_id
              @map[service_id] += 1
            else
              # When service id is not supplied, one of two things may be
              # happening;
              #
              # 1. The pool is not to a load balancer, in which case we only
              #    need to increment the generation for the nil service_id.
              # 2. The pool is to a load balancer, in which case we need to
              #    increment the generation for each service.
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
