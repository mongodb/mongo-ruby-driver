# Copyright (C) 2009-2013 MongoDB, Inc.
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

require 'mongo/functional/authentication'
require 'mongo/functional/logging'
require 'mongo/functional/read_preference'
require 'mongo/functional/write_concern'
require 'mongo/functional/uri_parser'

require 'mongo/functional/sasl_java' if RUBY_PLATFORM =~ /java/
