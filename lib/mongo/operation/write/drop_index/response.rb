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
  module Operation
    module Write
      class DropIndex

        # Response wrapper for drop index operations.
        #
        # @since 2.0.0
        class Response
          include Verifiable

          # Get the pretty formatted inspection of the response.
          #
          # @example Inspect the response.
          #   response.inspect
          #
          # @return [ String ] The inspection.
          #
          # @since 2.0.0
          def inspect
            "#<Mongo::Operation::Write::DropIndex::Response:#{object_id} documents=#{documents}>"
          end

          # Verify the response by checking for any errors.
          #
          # @example Verify the response.
          #   response.verify!
          #
          # @raise [ Write::Failure ] If an error is in the response.
          #
          # @return [ Response ] The response if verification passed.
          #
          # @since 2.0.0
          def verify!
            write_failure? ? raise(Write::Failure.new(first)) : self
          end
        end
      end
    end
  end
end
