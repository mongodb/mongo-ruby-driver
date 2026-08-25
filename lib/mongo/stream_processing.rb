# frozen_string_literal: true

# Copyright (C) 2026-present MongoDB Inc.
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
  # Atlas Stream Processing (ASP) workspace client and helpers.
  #
  # See {Mongo::StreamProcessing::Client} for the entry point.
  #
  # @since 2.25.0
  module StreamProcessing
  end
end

require 'mongo/stream_processing/processor_info'
require 'mongo/stream_processing/samples_result'
require 'mongo/stream_processing/processor'
require 'mongo/stream_processing/processors'
require 'mongo/stream_processing/client'
