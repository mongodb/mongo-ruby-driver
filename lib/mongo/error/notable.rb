# Copyright (C) 2019 MongoDB, Inc.
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
  class Error < StandardError

    # A module encapsulating note tracking functionality, since currently
    # the driver does not have a single exception hierarchy root.
    #
    # @since 2.11.0
    # @api private
    module Notable

      # Returns an array of strings with additional information about the
      # exception.
      #
      # @return [ Array<String> ] Additional information strings.
      #
      # @since 2.11.0
      # @api public
      def notes
        if @notes
          @notes.dup
        else
          []
        end
      end

      # @api private
      def add_note(note)
        unless @notes
          @notes = []
        end
        @notes << note
      end

      # @api public
      def message
        super + notes_tail
      end

      # @api public
      def to_s
        super + notes_tail
      end

      # @api public
      def inspect
        msg = super
        if msg.end_with?('>')
          msg[0...msg.length-1] + notes_tail + '>'
        else
          msg + notes_tail
        end
      end

      private

      # @api private
      def notes_tail
        msg = ''
        unless notes.empty?
          msg += " (#{notes.join(', ')})"
        end
        msg
      end
    end
  end
end
