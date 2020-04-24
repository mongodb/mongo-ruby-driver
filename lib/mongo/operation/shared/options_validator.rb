# Copyright (C) 2020 MongoDB Inc.
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

    # TODO: documentation
    module OptionsValidator
      private def validate_options!(connection)
        if !acknowledged_write?
          if collation
            raise Error::UnsupportedCollation.new(
                Error::UnsupportedCollation::UNACKNOWLEDGED_WRITES_MESSAGE)
          end

          if array_filters(connection)
            raise Error::UnsupportedArrayFilters.new(
                Error::UnsupportedArrayFilters::UNACKNOWLEDGED_WRITES_MESSAGE)
          end

          if hint
            # TODO: add message here
            raise Error::UnsupportedHint.new
          end
        end
      end
    end
  end
end
