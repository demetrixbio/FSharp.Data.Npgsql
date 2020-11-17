#!/bin/bash
set -e
docker ps -a
dotnet test Tests.sln
