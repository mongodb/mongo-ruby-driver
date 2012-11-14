# -*- mode: ruby; -*-

desc "Generate all documentation"
task :docs => 'docs:yard'

namespace :docs do

  desc "Generate yard documention"
  task :yard do
    version = bumper_version.to_s
    out = File.join('docs', version)
    FileUtils.rm_rf(out)
    system "yardoc -o #{out} --title mongo-#{version}"
  end

end