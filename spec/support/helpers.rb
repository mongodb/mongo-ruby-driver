# Copyright (C) 2009-2013 MongoDB, Inc.
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

# Helper methods and utilities for testing.
module Helpers

  # Helper method to allow temporary redirection of $stdout.
  #
  # @example
  # silence do
  #   # your noisey code here
  # end
  #
  # @param A code block to execute.
  # @return Original $stdout value.
  def silence(&block)
    original_stdout = $stdout
    original_stderr = $stderr
    $stdout = $stderr = File.new('/dev/null', 'w')
    yield block
  ensure
    $stdout = original_stdout
    $stderr = original_stderr
  end

end
