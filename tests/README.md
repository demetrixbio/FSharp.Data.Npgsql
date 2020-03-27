# Darren notes on setting up testing

You need to build a postgres-gis image (need v12 for compatibility with restore)


## Making the docker image
`docker build`  to create an image


## Running docker image and mapping postgres port to 32768 (avoids any local postgres instances)
`docker run -p 32768:5432 "-e POSTGRES_PASSWORD=postgres" <image id>`

This should start up and successfully restore the dvdrental database inside the docker image (copies in file then runs `pg_restore`.  If either of those steps fails you won't have a working database.

## Troubleshooting

If your tests are having trouble connecting, try connecting to db from commandline

E.g. for bash on windows

`winpty /c/Program\ Files/PostgreSQL/12/bin/psql -h 127.0.0.1 -U postgres -d dvdrental -p 32768`


## Updating test data

* Create working database
* Connect and alter schema

`$ winpty /c/Program\ Files/PostgreSQL/12/bin/pg_dump -U postgres -h 127.0.0.1 -p 32768 -F t -f dvdrentalnew.tar dvdrental`
