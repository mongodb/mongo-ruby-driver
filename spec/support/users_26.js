db.createUser(
  {
    user: "test-user",
    pwd: "password",
    roles:
    [
      {
        role: "userAdmin",
        db: "ruby-driver"
      }
    ]
  }
);
