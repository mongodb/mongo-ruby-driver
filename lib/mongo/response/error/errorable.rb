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
  module Response
    module Error

      # Methods for error classes.
      #
      # @since 2.0.0
      module Errorable

        # Initialize this error.
        #
        # @since 2.0.0
        def initialize(msg)
          @msg = msg
        end

        # Return an integer value identifying the error.
        #
        # @return [ Integer ] error code.
        #
        # @since 2.0.0
        def code
          0
        end

        # Return a description of this error.
        #
        # @return [ String ] error description.
        #
        # @since 2.0.0
        def errmsg
          "OMG LOOK OUT I'M AN ERROR!!!"
        end
      end
    end
  end
end
