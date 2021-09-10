# Local Kerberos

The scripts and configuration files in this directory provision a local
Kerberos server via Docker.

## Usage

Build the Docker image:

    docker build -t local-kerberos

Run the container with the Kerberos server:

    docker run -it --init local-kerberos

Note: the `--init` flag is important to be able to stop the container with
Ctrl-C.

The container by default tails the KDC log which should show authentication
attempts by clients.

When the container starts, it prints the instructions that need to be followed
to use it, including its IP address. For convenience the instructions are
repeated below.

1. Add the container's IP address to `/etc/hosts` on the host machine.
For example, if the container's IP address is `172.17.0.3`, run:

     echo 172.17.0.3 krb.local | sudo tee -a /etc/hosts

2. Install `krb5-user` on the host machine:

     sudo apt-get install krb5-user

  This step may vary based on the host operating system.

3.  Create `/etc/krb5.conf` with the contents of `krb5.conf` in this directory.

4. Log in using `kinit`:

    kinit test/test@LOCALKRB

  The password is `testp`.

## References

The following resources were used to develop the provisioner:

- [Kerberos instructions for Ubuntu](https://help.ubuntu.com/lts/serverguide/kerberos.html)
- [Kerberos upstream instructions for configuring a KDC](https://web.mit.edu/kerberos/krb5-devel/doc/admin/install_kdc.html)
- [kadm5.acl syntax](https://web.mit.edu/kerberos/krb5-devel/doc/admin/conf_files/kadm5_acl.html#kadm5-acl-5)
- [Kerberos instructions for RHEL](https://www.rootusers.com/how-to-configure-linux-to-authenticate-using-kerberos/)
