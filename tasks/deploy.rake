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

desc "Installs gems from source"
task :install => ['deploy:package'] do
  Dir.glob('*.gem').each { |file| "gem install #{file}" }
  Rake::Task['deploy:cleanup']
end

desc "Runs default deploy task (release)"
task :deploy => 'deploy:release'

namespace :deploy do
  VERSION_MATCH = /"\d\.\d\.\d{1,3}"|"\d\.\d\.\d{1,3}\.\w*"|"\d\.\d\.\d{1,3}\-\w*"/
  def update_version(value=bumper_version.to_s)
    filename = File.join(File.dirname(__FILE__), '../ext/cbson/version.h')
    content = File.read(filename)
    raise ArgumentError unless "\"#{value}\"" =~ VERSION_MATCH
    File.open(filename, 'w') do |f|
      f.write(content.gsub!(VERSION_MATCH, "\"#{value}\""))
    end
  end

  desc "Bump the version or specify a custom version (revision by default)"
  task :version, :custom do |t, args|
    if args[:custom]
      update_version(args[:custom])
      filename = File.join(File.dirname(__FILE__), '../VERSION')
      File.open(filename, 'w') {|f| f.write(args[:custom])}
      puts "version: #{args[:custom]}"
    else
      Rake::Task['bump:revision'].invoke
      update_version
    end
  end

  desc "Tag with version and push to github"
  task :git do
    g = Git.open(Dir.getwd())
    g.add(['VERSION', 'ext/cbson/version.h'])
    g.commit "RELEASE #{bumper_version}"
    g.add_tag("#{bumper_version}")
    g.push('origin', '1.x-stable', true)
  end

  desc "Package all gems for release"
  task :package do
    Dir.glob('*.gemspec').each { |file| system "gem build #{file}" }
  end

  desc "Release all the things!"
  task :release do
    Dir.glob('*.gem').each { |file| system "gem push #{file}" }
    Rake::Task['deploy:cleanup'].invoke
  end

  task :cleanup do
    puts '[CLEAN-UP] Removing gem files...'
    Dir.glob('*.gem').each { |file| File.delete(file) if File.exists?(file) }
  end

end
