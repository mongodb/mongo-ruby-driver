# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2016-2020 MongoDB Inc.
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
  # This module abstracts the functionality for generating sequential
  # unique integer IDs for instances of the class. It defines the method
  # #next_id on the class that includes it. The implementation ensures that
  # the IDs will be unique even when called from multiple threads.
  #
  # @example Include the Id module.
  #   class Foo
  #     include Mongo::Id
  #   end
  #
  #   f = Foo.new
  #   foo.next_id # => 1
  #   foo.next_id # => 2
  #
  # Classes which include Id should _not_ access `@@id` or `@@id_lock`
  # directly; instead, they should call `#next_id` in `#initialize` and save
  # the result in the instance being created.
  #
  # @example Save the ID in the instance of the including class.
  #   class Bar
  #     include Mongo::Id
  #
  #     attr_reader :id
  #
  #     def initialize
  #       @id = self.class.next_id
  #     end
  #   end
  #
  #   a = Bar.new
  #   a.id # => 1
  #   b = Bar.new
  #   b.id # => 2
  #
  # @since 2.7.0
  # @api private
  module Id
    def self.included(klass)
      klass.class_variable_set(:@@id, 0)
      klass.class_variable_set(:@@id_lock, Mutex.new)

      klass.define_singleton_method(:next_id) do
        klass.class_variable_get(:@@id_lock).synchronize do
          id = class_variable_get(:@@id)
          klass.class_variable_set(:@@id, id + 1)
          klass.class_variable_get(:@@id)
        end
      end
    end
  end
end
