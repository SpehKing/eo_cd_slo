# Use TimescaleDB with PostgreSQL 17 as base image
FROM timescale/timescaledb-ha:pg17

# Set environment variables for PostgreSQL
ENV POSTGRES_DB=eo_db
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=password

# Copy initialization SQL scripts
COPY ./db/docker-entrypoint-initdb.d/ /docker-entrypoint-initdb.d/

# Expose PostgreSQL port
EXPOSE 5432

# Create a volume for PostgreSQL data
VOLUME ["/var/lib/postgresql/data"]

# The base image already has the correct entrypoint and cmd
# so we don't need to specify them explicitly
