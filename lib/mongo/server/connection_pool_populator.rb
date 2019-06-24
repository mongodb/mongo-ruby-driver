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
  class Server

  	# A manager that maintains the invariant that the
  	# size of a connection pool is at least minPoolSize.
  	#
  	# @api private
  	class ConnectionPoolPopulator
  	  def initialize(pool, available_semaphore, request_semaphore)
  	  	@pool = pool
  	  	@available_semaphore = available_semaphore
  	  	@request_semaphore = request_semaphore
  	  end

  	  def run!
  	  	@thread = Thread.new {
  	  	  while !@pool.closed? do
  	  	  	@pool.populate
  	  	  	@request_semaphore.wait(5)	# TODO what should be timeout and why? for when pool closes?
  	  	  end
  	  	}
  	  end
  	end
  end
end