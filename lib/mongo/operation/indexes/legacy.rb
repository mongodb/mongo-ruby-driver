# Copyright (C) 2014-2020 MongoDB Inc.
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
    class Indexes

      # A MongoDB indexes operation sent as a legacy wire protocol message.
      #
      # @api private
      #
      # @since 2.5.2
      class Legacy
        include Specifiable
        include Executable
        include ReadPreferenceSupported

        private

        def selector(connection)
          { ns: namespace }
        end

        def message(connection)
          Protocol::Query.new(db_name, Index::COLLECTION, command(connection), options(connection))
        end

        def result_class
          Operation::Result
        end
      end
    end
  end
end
