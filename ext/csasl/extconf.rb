require 'mkmf'
find_header('sasl/sasl.h')

if have_library('sasl2', 'sasl_version')
  create_makefile('csasl/csasl')
else
  abort "libsasl is required in the system to install the mongo-kerberos gem."
end
