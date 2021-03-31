# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2015-2020 MongoDB Inc.
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

require 'mongo/operation/indexes/command'
require 'mongo/operation/indexes/op_msg'
require 'mongo/operation/indexes/legacy'
require 'mongo/operation/indexes/result'

module Mongo
  module Operation

    # A MongoDB indexes operation.
    #
    # @api private
    #
    # @since 2.0.0
    class Indexes
      include Specifiable
      include PolymorphicOperation
      include PolymorphicLookup

      private

      def final_operation(connection)
        cls = if connection.features.op_msg_enabled?
          polymorphic_class(self.class.name, :OpMsg)
        elsif connection.features.list_indexes_enabled?
          polymorphic_class(self.class.name, :Command)
        else
          polymorphic_class(self.class.name, :Legacy)
        end
        cls.new(spec)
      end
    end
  end
end
