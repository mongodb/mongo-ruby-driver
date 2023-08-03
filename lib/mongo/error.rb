# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

require 'mongo/error/notable'
require 'mongo/error/labelable'

module Mongo
  # Base error class for all Mongo related errors.
  #
  # @since 2.0.0
  class Error < StandardError
    include Notable
    include Labelable

    # The error code field.
    #
    # @since 2.0.0
    CODE = 'code'.freeze

    # An error field, MongoDB < 2.6
    #
    # @since 2.0.0
    # @deprecated
    ERR = '$err'.freeze

    # An error field, MongoDB < 2.4
    #
    # @since 2.0.0
    # @deprecated
    ERROR = 'err'.freeze

    # The standard error message field, MongoDB 3.0+
    #
    # @since 2.0.0
    # @deprecated
    ERRMSG = 'errmsg'.freeze

    # The constant for the writeErrors array.
    #
    # @since 2.0.0
    # @deprecated
    WRITE_ERRORS = 'writeErrors'.freeze

    # The constant for a write concern error.
    #
    # @since 2.0.0
    # @deprecated
    WRITE_CONCERN_ERROR = 'writeConcernError'.freeze

    # The constant for write concern errors.
    #
    # @since 2.1.0
    # @deprecated
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
    # @deprecated
    UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL = 'UnknownTransactionCommitResult'.freeze

    # Error label describing errors that will likely not occur if a transaction is manually retried
    # from the start.
    #
    # @since 2.6.0
    # @deprecated
    TRANSIENT_TRANSACTION_ERROR_LABEL = 'TransientTransactionError'.freeze

    def initialize(msg = nil)
      super
      @write_concern_error_labels = []
    end

    # Does the write concern error have the given label?
    #
    # @param [ String ] label The label to check for the presence of.
    #
    # @return [ Boolean ] Whether the write concern error has the given label.
    def write_concern_error_label?(label)
      @write_concern_error_labels.include?(label)
    end

    # The set of error labels associated with the write concern error.
    #
    # @return [ Array<String> ] The list of error labels.
    def write_concern_error_labels
      @write_concern_error_labels.dup
    end
  end
end

require 'mongo/error/auth_error'
require 'mongo/error/bad_load_balancer_target'
require 'mongo/error/sdam_error_detection'
require 'mongo/error/parser'
require 'mongo/error/write_retryable'
require 'mongo/error/change_stream_resumable'
require 'mongo/error/bulk_write_error'
require 'mongo/error/client_closed'
require 'mongo/error/closed_stream'
require 'mongo/error/connection_check_out_timeout'
require 'mongo/error/connection_perished'
require 'mongo/error/connection_unavailable'
require 'mongo/error/credential_check_error'
require 'mongo/error/crypt_error'
require 'mongo/error/extra_file_chunk'
require 'mongo/error/file_not_found'
require 'mongo/error/handshake_error'
require 'mongo/error/invalid_address'
require 'mongo/error/invalid_bulk_operation'
require 'mongo/error/invalid_bulk_operation_type'
require 'mongo/error/invalid_collection_name'
require 'mongo/error/invalid_config_option'
require 'mongo/error/invalid_cursor_operation'
require 'mongo/error/invalid_database_name'
require 'mongo/error/invalid_document'
require 'mongo/error/invalid_file'
require 'mongo/error/invalid_file_revision'
require 'mongo/error/invalid_max_connecting'
require 'mongo/error/invalid_min_pool_size'
require 'mongo/error/invalid_read_option'
require 'mongo/error/invalid_application_name'
require 'mongo/error/invalid_nonce'
require 'mongo/error/invalid_read_concern'
require 'mongo/error/invalid_replacement_document'
require 'mongo/error/invalid_server_auth_response'
# Subclass of InvalidServerAuthResponse
require 'mongo/error/invalid_server_auth_host'
require 'mongo/error/invalid_server_preference'
require 'mongo/error/invalid_session'
require 'mongo/error/invalid_signature'
require 'mongo/error/invalid_transaction_operation'
require 'mongo/error/invalid_txt_record'
require 'mongo/error/invalid_update_document'
require 'mongo/error/invalid_uri'
require 'mongo/error/invalid_write_concern'
require 'mongo/error/insufficient_iteration_count'
require 'mongo/error/internal_driver_error'
require 'mongo/error/kms_error'
require 'mongo/error/lint_error'
require 'mongo/error/max_bson_size'
require 'mongo/error/max_message_size'
require 'mongo/error/mismatched_domain'
require 'mongo/error/mongocryptd_spawn_error'
require 'mongo/error/multi_index_drop'
require 'mongo/error/need_primary_server'
require 'mongo/error/no_service_connection_available'
require 'mongo/error/no_server_available'
require 'mongo/error/no_srv_records'
require 'mongo/error/session_ended'
require 'mongo/error/sessions_not_supported'
require 'mongo/error/session_not_materialized'
require 'mongo/error/snapshot_session_invalid_server_version'
require 'mongo/error/snapshot_session_transaction_prohibited'
require 'mongo/error/operation_failure'
require 'mongo/error/pool_error'
require 'mongo/error/pool_closed_error'
require 'mongo/error/pool_paused_error'
require 'mongo/error/raise_original_error'
require 'mongo/error/server_certificate_revoked'
require 'mongo/error/socket_error'
require 'mongo/error/pool_cleared_error'
require 'mongo/error/socket_timeout_error'
require 'mongo/error/failed_string_prep_validation'
require 'mongo/error/unchangeable_collection_option'
require 'mongo/error/unexpected_chunk_length'
require 'mongo/error/unexpected_response'
require 'mongo/error/missing_connection'
require 'mongo/error/missing_file_chunk'
require 'mongo/error/missing_password'
require 'mongo/error/missing_resume_token'
require 'mongo/error/missing_scram_server_signature'
require 'mongo/error/missing_service_id'
require 'mongo/error/server_api_conflict'
require 'mongo/error/server_api_not_supported'
require 'mongo/error/server_not_usable'
require 'mongo/error/unknown_payload_type'
require 'mongo/error/unmet_dependency'
require 'mongo/error/unsupported_option'
require 'mongo/error/unsupported_array_filters'
require 'mongo/error/unsupported_collation'
require 'mongo/error/unsupported_features'
require 'mongo/error/unsupported_message_type'
