#!/bin/bash
set -e

PGPASSWORD=postgres pg_restore -h localhost -p 32768 -U postgres -d dvdrental -F t ./tests/dvdrental.tar
dotnet test Tests.sln
