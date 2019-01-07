# Copyright (C) 2016-2019  MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  # This module abstracts the functionality for generating sequential unique integer IDs for
  # instances of the class. It defines the method #next_id on the class that includes it. The
  # implementation ensures that the IDs will be unique even when called from multiple threads.
  #
  # @example Define and use the Id module.
  #   class Foo
  #     include Id
  #   end
  #
  #   f = Foo.new
  #   foo.next_id # => 1
  #   foo.next_id # => 2
  #
  # @since 2.7.0
  module Id
    def self.included(klass)
      klass.class_variable_set(:@@id, 1)
      klass.class_variable_set(:@@id_lock, Mutex.new)

      klass.define_singleton_method(:next_id) do
        klass.class_variable_get(:@@id_lock).synchronize do
          id = klass.class_variable_get(:@@id)
          klass.class_variable_set(:@@id, id + 1)
          id
        end
      end
    end
  end
end
