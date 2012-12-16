# -*- mode: ruby; -*-

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