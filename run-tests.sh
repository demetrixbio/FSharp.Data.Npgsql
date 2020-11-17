#!/bin/bash
set -e
docker logs tests_db_1
dotnet test Tests.sln
