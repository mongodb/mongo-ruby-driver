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

require 'rubygems'

begin
  require 'bundler'
rescue LoadError
  raise '[FAIL] Bundler not found! Install it with `gem install bundler && bundle`.'
end

rake_tasks = Dir.glob(File.join('tasks', '**', '*.rake')).sort
if ENV.keys.any? { |k| k.end_with?('_CI') }
  Bundler.require(:default, :testing)
  rake_tasks.reject! { |r| r =~ /deploy/ }
else
  Bundler.require(:default, :testing, :deploy, :development)
end

rake_tasks.each { |rake| load File.expand_path(rake) }
