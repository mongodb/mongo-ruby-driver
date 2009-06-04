require 'mkmf'

find_header("st.h")
find_header("regex.h")
dir_config('cbson')
create_makefile('mongo_ext/cbson')
