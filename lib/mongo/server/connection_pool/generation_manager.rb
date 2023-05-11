# frozen_string_literal: true
# rubocop:todo all

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
          @pipe_fds = Hash.new { |hash, key| hash[key] = { 1 => IO.pipe } }
          @server = server
          @lock = Mutex.new
          @scheduled_for_close = []
        end

        attr_reader :server

        def generation(service_id: nil)
          validate_service_id!(service_id)

          @lock.synchronize do
            @map[service_id]
          end
        end

        def generation_unlocked(service_id: nil)
          validate_service_id!(service_id)

          @map[service_id]
        end

        def pipe_fds(service_id: nil)
          @pipe_fds[service_id][@map[service_id]]
        end

        def remove_pipe_fds(generation, service_id: nil)
          validate_service_id!(service_id)

          r, w = @pipe_fds[service_id].delete(generation)
          w.close
          # Schedule the read end of the pipe to be closed. We cannot close it
          # immediately since we need to wait for any Kernel#select calls to
          # notice that part of the pipe is closed, and check the socket. This
          # all happens when attempting to read from the socket and waiting for
          # it to become ready again.
          @scheduled_for_close << r
        end

        def bump(service_id: nil)
          @lock.synchronize do
            close_all_scheduled
            if service_id
              gen = @map[service_id] += 1
              @pipe_fds[service_id] ||= {}
              @pipe_fds[service_id][gen] = IO.pipe
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
                gen = @map[k] += 1
                @pipe_fds[service_id] ||= {}
                @pipe_fds[service_id][gen] = IO.pipe
              end
            end
          end
        end

        private

        def validate_service_id!(service_id)
          if service_id
            unless server.load_balancer?
              raise ArgumentError, "Generation scoping to services is only available in load-balanced mode, but the server at #{server.address} is not a load balancer"
            end
          else
            if server.load_balancer?
              raise ArgumentError, "The server at #{server.address} is a load balancer and therefore does not have a single global generation"
            end
          end
        end

        # Close all fds scheduled for closing.
        def close_all_scheduled
          while pipe = @scheduled_for_close.pop
            pipe.close
          end
        end
      end
    end
  end
end
