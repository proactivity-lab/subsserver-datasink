# subsserver-datasink
The subsdatasink receives data from subsserver with subsservers
simple-data-sink protocol and stores it in a postgres database.

The webservice API is located at:
http://HOSTNAME:PORT/api/v0/sink/DATABASE

When installed with pip, an executable script is also installed.

`subsdatasink -h`

Configuration file for the datasink can be specified with `-c`.

Database must be created manually and must have INSERT permissions for
the user used by datasink.
