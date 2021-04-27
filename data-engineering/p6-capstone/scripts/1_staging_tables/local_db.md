docker pull dpage/pgadmin4 docker run -p 6020:80 -e 'PGADMIN_DEFAULT_EMAIL=user@domain.com' -e '
PGADMIN_DEFAULT_PASSWORD=SuperSecret' -d dpage/pgadmin4

http://localhost:6020/browser/

docker pull postgres

docker run -p 6021:5432 --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword -d postgres

# Docker

docker run -p 6021:5432 --name pg_postgres -e POSTGRES_PASSWORD=docker -d -v ./data:/var/lib/postgresql/data postgres
docker run -p 6020:80 --name pg_pgadmin -e 'PGADMIN_DEFAULT_EMAIL=user@domain.com' -e '
PGADMIN_DEFAULT_PASSWORD=SuperSecret' -d dpage/pgadmin4

# Login

http://localhost:6020/
user@domain.com SuperSecret

postgres:docker