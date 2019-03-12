# Copyright (C) 2019 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'mongo/monitoring/event/cmap/base'
require 'mongo/monitoring/event/cmap/connection_checked_in'
require 'mongo/monitoring/event/cmap/connection_checked_out'
require 'mongo/monitoring/event/cmap/connection_check_out_failed'
require 'mongo/monitoring/event/cmap/connection_check_out_started'
require 'mongo/monitoring/event/cmap/connection_closed'
require 'mongo/monitoring/event/cmap/connection_created'
require 'mongo/monitoring/event/cmap/connection_ready'
require 'mongo/monitoring/event/cmap/pool_cleared'
require 'mongo/monitoring/event/cmap/pool_closed'
require 'mongo/monitoring/event/cmap/pool_created'
