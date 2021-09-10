#!/bin/sh

echo 127.0.0.1 krb.local >>/etc/hosts

krb5kdc
kadmind

# Check that the daemons are running:
#ps awwxu

# Check that kerberos is set up successfully and a user can authenticate:
echo testp |kinit test/test@LOCALKRB
echo Authentication test succeeded

if ! grep docker-init /proc/1/cmdline; then
  echo
  echo NOTE: container is running without --init. Ctrl-C will not stop it.
fi

ip=`ip a |grep eth0 |grep inet |awk '{print $2}' |sed -e 's,/.*,,'`

echo
echo '==================================================================='
echo
echo To use this container for Kerberos authentication:
echo
echo 1. Add its IP address, $ip, to /etc/hosts:
echo
echo "     echo $ip krb.local | sudo tee -a /etc/hosts"
echo
echo 2. Install krb5-user:
echo
echo '     sudo apt-get install krb5-user'
echo
echo 3. Create /etc/krb5.conf with the following contents:
echo
cat /etc/krb5.conf |sed -e 's/^/    /'
echo
echo "4. Log in using kinit with the password 'testp':"
echo
echo '    kinit test/test@LOCALKRB'
echo
echo '==================================================================='
echo

# sudo apt-get install krb5-user

exec "$@"
