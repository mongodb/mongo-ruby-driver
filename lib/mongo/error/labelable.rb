# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2022 MongoDB Inc.
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
  class Error

    # A module encapsulating functionality to manage labels added to errors.
    #
    # @note Although methods of this module are part of the public API,
    #   the fact that these methods are defined on this module and not on
    #   the classes which include this module is not part of the public API.
    #
    # @api semipublic
    module Labelable

      # Does the error have the given label?
      #
      # @example
      #   error.label?(label)
      #
      # @param [ String ] label The label to check if the error has.
      #
      # @return [ true, false ] Whether the error has the given label.
      #
      # @since 2.6.0
      def label?(label)
        @labels && @labels.include?(label)
      end

      # Gets the set of labels associated with the error.
      #
      # @example
      #   error.labels
      #
      # @return [ Array ] The set of labels.
      #
      # @since 2.7.0
      def labels
        if @labels
          @labels.dup
        else
          []
        end
      end

      # Adds the specified label to the error instance, if the label is not
      # already in the set of labels.
      #
      # @param [ String ] label The label to add.
      #
      # @api private
      def add_label(label)
        @labels ||= []
        @labels << label unless label?(label)
      end
    end
  end
end
