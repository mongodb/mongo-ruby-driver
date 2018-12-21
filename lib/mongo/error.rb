# Copyright (C) 2014-2019 MongoDB, Inc.
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

require 'forwardable'

module Mongo
  # Base error class for all Mongo related errors.
  #
  # @since 2.0.0
  class Error < StandardError

    # The error code field.
    #
    # @since 2.0.0
    CODE = 'code'.freeze

    # An error field, MongoDB < 2.6
    #
    # @since 2.0.0
    ERR = '$err'.freeze

    # An error field, MongoDB < 2.4
    #
    # @since 2.0.0
    ERROR = 'err'.freeze

    # The standard error message field, MongoDB 3.0+
    #
    # @since 2.0.0
    ERRMSG = 'errmsg'.freeze

    # The constant for the writeErrors array.
    #
    # @since 2.0.0
    WRITE_ERRORS = 'writeErrors'.freeze

    # The constant for a write concern error.
    #
    # @since 2.0.0
    WRITE_CONCERN_ERROR = 'writeConcernError'.freeze

    # The constant for write concern errors.
    #
    # @since 2.1.0
    WRITE_CONCERN_ERRORS = 'writeConcernErrors'.freeze

    # Constant for an unknown error.
    #
    # @since 2.0.0
    UNKNOWN_ERROR = 8.freeze

    # Constant for a bad value error.
    #
    # @since 2.0.0
    BAD_VALUE = 2.freeze

    # Constant for a Cursor not found error.
    #
    # @since 2.2.3
    CURSOR_NOT_FOUND = 'Cursor not found.'

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
      false
    end

    # Error label describing commitTransaction errors that may or may not occur again if a commit is
    # manually retried by the user.
    #
    # @since 2.6.0
    UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL = 'UnknownTransactionCommitResult'.freeze

    # Error label describing errors that will likely not occur if a transaction is manually retried
    # from the start.
    #
    # @since 2.6.0
    TRANSIENT_TRANSACTION_ERROR_LABEL = 'TransientTransactionError'.freeze

    def initialize(msg = nil)
      @labels ||= []
      super(msg)
    end

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
      @labels.include?(label)
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
      @labels.dup
    end

    private

    def add_label(label)
      @labels << label unless label?(label)
    end
  end
end

require 'mongo/error/parser'
require 'mongo/error/write_retryable'
require 'mongo/error/change_stream_resumable'
require 'mongo/error/bulk_write_error'
require 'mongo/error/closed_stream'
require 'mongo/error/extra_file_chunk'
require 'mongo/error/file_not_found'
require 'mongo/error/operation_failure'
require 'mongo/error/invalid_bulk_operation'
require 'mongo/error/invalid_bulk_operation_type'
require 'mongo/error/invalid_collection_name'
require 'mongo/error/invalid_database_name'
require 'mongo/error/invalid_document'
require 'mongo/error/invalid_file'
require 'mongo/error/invalid_file_revision'
require 'mongo/error/invalid_min_pool_size'
require 'mongo/error/invalid_read_option'
require 'mongo/error/invalid_application_name'
require 'mongo/error/invalid_nonce'
require 'mongo/error/invalid_replacement_document'
require 'mongo/error/invalid_server_preference'
require 'mongo/error/invalid_session'
require 'mongo/error/invalid_signature'
require 'mongo/error/invalid_transaction_operation'
require 'mongo/error/invalid_txt_record'
require 'mongo/error/invalid_update_document'
require 'mongo/error/invalid_uri'
require 'mongo/error/invalid_write_concern'
require 'mongo/error/insufficient_iteration_count'
require 'mongo/error/lint_error'
require 'mongo/error/max_bson_size'
require 'mongo/error/max_message_size'
require 'mongo/error/mismatched_domain'
require 'mongo/error/multi_index_drop'
require 'mongo/error/need_primary_server'
require 'mongo/error/no_server_available'
require 'mongo/error/no_srv_records'
require 'mongo/error/socket_error'
require 'mongo/error/socket_timeout_error'
require 'mongo/error/failed_stringprep_validation'
require 'mongo/error/unchangeable_collection_option'
require 'mongo/error/unexpected_chunk_length'
require 'mongo/error/unexpected_response'
require 'mongo/error/missing_file_chunk'
require 'mongo/error/missing_resume_token'
require 'mongo/error/unsupported_array_filters'
require 'mongo/error/unknown_payload_type'
require 'mongo/error/unsupported_collation'
require 'mongo/error/unsupported_features'
require 'mongo/error/unsupported_message_type'
require 'mongo/error/transaction_aborted'
