# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2020 MongoDB Inc.
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

    # A module encapsulating functionality to manage data attached to
    # exceptions in the driver, since the driver does not currently have a
    # single exception hierarchy root.
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
        if Lint.enabled?
          if @notes.include?(note)
            # The driver makes an effort to not add duplicated notes, by
            # keeping track of *when* a particular exception should have the
            # particular notes attached to it throughout the call stack.
            raise Error::LintError, "Adding a note which already exists in exception #{self}: #{note}"
          end
        end
        @notes << note
      end

      # Allows multiple notes to be added in a single call, for convenience.
      #
      # @api private
      def add_notes(*notes)
        notes.each { |note| add_note(note) }
      end

      # Returns connection pool generation for the connection on which the
      # error occurred.
      #
      # @return [ Integer | nil ] Connection pool generation.
      attr_accessor :generation

      # Returns service id for the connection on which the error occurred.
      #
      # @return [ Object | nil ] Service id.
      #
      # @api experimental
      attr_accessor :service_id

      # Returns global id of the connection on which the error occurred.
      #
      # @return [ Integer | nil ] Connection global id.
      #
      # @api private
      attr_accessor :connection_global_id

      # @api public
      def to_s
        super + notes_tail
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
