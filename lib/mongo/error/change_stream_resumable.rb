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
  class Error

    # A module signifying the error will always cause change stream to
    # resume once.
    #
    # @since 2.6.0
    module ChangeStreamResumable
      # Can the change stream on which this error occurred be resumed,
      # provided the operation that triggered this error was a getMore?
      #
      # @example Is the error resumable for the change stream?
      #   error.change_stream_resumable?
      #
      # @return [ true, false ] Whether the error is resumable.
      #
      # @since 2.6.0
      def change_stream_resumable?
        true
      end
    end
  end
end
