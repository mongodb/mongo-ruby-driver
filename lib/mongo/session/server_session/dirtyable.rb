# frozen_string_literal: true

# Copyright (C) 2024 MongoDB Inc.
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
  class Session
    class ServerSession
      # Functionality for manipulating and querying a session's
      # "dirty" state, per the last paragraph at
      # https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#server-session-pool
      #
      #   If a driver has a server session pool and a network error is
      #   encountered when executing any command with a ClientSession, the
      #   driver MUST mark the associated ServerSession as dirty. Dirty server
      #   sessions are discarded when returned to the server session pool. It is
      #   valid for a dirty session to be used for subsequent commands (e.g. an
      #   implicit retry attempt, a later command in a bulk write, or a later
      #   operation on an explicit session), however, it MUST remain dirty for
      #   the remainder of its lifetime regardless if later commands succeed.
      #
      # @api private
      module Dirtyable
        # Query whether the server session has been marked dirty or not.
        #
        # @return [ true | false ] the server session's dirty state
        def dirty?
          @dirty
        end

        # Mark the server session as dirty (the default) or clean.
        #
        # @param [ true | false ] mark whether the mark the server session
        #   dirty or not.
        def dirty!(mark = true)
          @dirty = mark
        end
      end
    end
  end
end
