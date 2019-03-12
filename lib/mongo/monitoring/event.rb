# Copyright (C) 2015-2019 MongoDB, Inc.
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

require 'mongo/event'
require 'mongo/monitoring/event/secure'
require 'mongo/monitoring/event/command_started'
require 'mongo/monitoring/event/command_succeeded'
require 'mongo/monitoring/event/command_failed'
require 'mongo/monitoring/event/cmap'
require 'mongo/monitoring/event/server_closed'
require 'mongo/monitoring/event/server_description_changed'
require 'mongo/monitoring/event/server_opening'
require 'mongo/monitoring/event/server_heartbeat_started'
require 'mongo/monitoring/event/server_heartbeat_succeeded'
require 'mongo/monitoring/event/server_heartbeat_failed'
require 'mongo/monitoring/event/topology_changed'
require 'mongo/monitoring/event/topology_closed'
require 'mongo/monitoring/event/topology_opening'
