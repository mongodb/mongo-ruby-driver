# Copyright (C) 2013 10gen Inc.
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

if RUBY_PLATFORM =~ /java/
  require 'jruby-jars'
  Rake::JavaExtensionTask.new('jbson') do |ext|
    ext.ext_dir = 'ext/jbson'
    ext.lib_dir = ext.tmp_dir = 'ext/jbson/target'
    jars = ['ext/jbson/lib/java-bson.jar', JRubyJars.core_jar_path]
    ext.classpath = jars.map { |x| File.expand_path x }.join(':')
    Rake::Task['clean'].invoke
  end
else
  Rake::ExtensionTask.new('cbson') do |ext|
    ext.lib_dir = "lib/bson_ext"
    Rake::Task['clean'].invoke
  end
end

desc "Run the default compile task"
task :compile => RUBY_PLATFORM =~ /java/ ? 'compile:jbson' : 'compile:cbson'