#!/usr/bin/env ruby

require 'rubygems'

begin
  require 'bundler'
rescue LoadError
  raise '[FAIL] Bundler not found! Install it with ' +
        '`gem install bundler; bundle install`.'
end

default_groups = [:default, :testing]
default_groups << :release unless ENV['TEST']
Bundler.require(*default_groups)

Dir.glob('tasks/**/*.rake').sort.each { |r| load r }
