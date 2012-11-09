# -*- mode: ruby; -*-

Rake::ExtensionTask.new('cbson') do |ext|
  ext.lib_dir = "lib/bson_ext"
end

# Rake::JavaExtensionTask.new('jbson') do |ext|
# end

desc "Runs default compile tasks (cbson, jbson)"
task :compile => ['compile:jbson', 'compile:cbson', 'clobber']

namespace :compile do
  desc "Compile jbson"
  task :jbson do
    java_dir  = File.join(File.dirname(__FILE__), 'ext', 'java')
    jar_dir   = File.join(java_dir, 'jar')
    jruby_jar = File.join(jar_dir, 'jruby.jar')
    mongo_jar = File.join(jar_dir, 'mongo-2.6.5.jar')
    src_base   = File.join(java_dir, 'src')
    system("javac -Xlint:deprecation -Xlint:unchecked -classpath #{jruby_jar}:#{mongo_jar} #{File.join(src_base, 'org', 'jbson', '*.java')}")
    system("cd #{src_base} && jar cf #{File.join(jar_dir, 'jbson.jar')} #{File.join('.', 'org', 'jbson', '*.class')}")
  end
end