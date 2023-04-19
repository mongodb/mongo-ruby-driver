# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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

    # Shared behavior of instantiating a result class matching the
    # operation class.
    #
    # This module must be included after Executable module because result_class
    # is defined in both.
    #
    # @api private
    module PolymorphicResult
      include PolymorphicLookup

      private

      def self.included(base)
        base.extend ClassMethods
      end

      module ClassMethods
        attr_accessor :result_class
      end

      def result_class
        self.class.result_class ||= begin
          polymorphic_class(self.class.name, :Result)
        rescue NameError
          polymorphic_class(self.class.name.sub(/::[^:]*$/, ''), :Result)
        end
      end
    end
  end
end
