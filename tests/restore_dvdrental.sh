#!/bin/bash 
pg_restore -U postgres -d dvdrental --schema public -F t /var/lib/postgresql/backup/dvdrental.tar
