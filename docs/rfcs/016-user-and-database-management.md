## Zentih user and database management RFC

### CREATE USER

CREATE USER is only allowed from console, 
because it requires generation of the access token and other complex logic.

Client has to chose which databases will be accessible by the new user.
Along with database access, we explicitly grant CREATE ON DATABASE privilege to the user.
`GRANT CREATE ON DATABASE .. TO .. ;`
to let the user to create extensions.

All new users are granted with CREATEDB privilege by default.
`CREATE ROLE .. WITH CREATEDB`
Later we may add an option to manage it.

*Q: Are we going to allow user to create new users and NOLIGIN roles to manage privileges?
*`CREATE ROLE .. WITH CREATEROLE`


### CREATE DATABASE

Client can CREATE DATABASE via SQL as well as via console.

When database is created from SQL (not by superuser)
we intercept this action using DDL hook and send it to the console to synchronize the information about databases.

### CREATE EXTENSION

To create trusted extensions in the database, user must have an access privileges on the database.

We explicitly grant CREATE ON DATABASE privilege to the new user along with database access.
If user creates a database, he becomes the owner and thus has CREATE ON DATABASE privilege by default.

### DROP EXTENSION

Nothing special. PostgreSQL behavior.

### DROP DATABASE

From PostgreSQL docs:
The right to drop an object, or to alter its definition in any way, is not treated as a grantable privilege; it is inherent in the owner, and cannot be granted or revoked. (However, a similar effect can be obtained by granting or revoking membership in the role that owns the object) 

Note that ownership can be changed using `REASSIGN OWNED BY .. TO ..` 

### DROP USER

DROP USER is only allowed from console.

In case some objects depend on the user, PostgreSQL throws a detailed error.

- ERROR:  role "p" cannot be dropped because some objects depend on it
DETAIL:  owner of database pdb
1 object in database pdb

### Competitiors

Competing databases have more or less standart approach to user management.
There is an admin/master user that can manage other roles and databases.

https://docs.microsoft.com/en-us/azure/postgresql/howto-create-users
the server admin user is granted these privileges: 
LOGIN, NOSUPERUSER, INHERIT, CREATEDB, CREATEROLE, REPLICATION

https://aws.amazon.com/blogs/database/managing-postgresql-users-and-roles/
The master user that is created during Amazon RDS and Aurora PostgreSQL instance creation should be used only for database administration tasks like creating other users, roles, and databases. 

https://cloud.google.com/sql/docs/postgres/users