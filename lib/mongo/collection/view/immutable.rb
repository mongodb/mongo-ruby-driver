# Copyright (C) 2014-2015 MongoDB, Inc.
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
  class Collection
    class View
      module Immutable

        # @return [ Hash ] options The additional query options.
        attr_reader :options

        private

        # @api private
        #
        # @note In the including class, the method #immutable needs to be
        #   implemented in order to define how a new class of that type needs to
        #   be instantiated.
        def configure(field, value)
          return options[field] if value.nil?
          new(options.merge(field => value))
        end

        def configure_flag(flag)
          new(options.dup).tap do |view|
            view.send(:flags).push(flag)
          end
        end
      end
    end
  end
end
