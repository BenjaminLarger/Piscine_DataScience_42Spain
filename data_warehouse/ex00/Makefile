SHELL	= /bin/sh

NAME	= piscine_data-science

all:
	docker compose up --build
test_postgres:
	docker exec -it postgres sh \
		-c "psql -U blarger -d piscineds -h localhost -W"
logs:
	docker-compose logs postgres
down:
	docker-compose down
rm_volume:
	docker-compose down && \
	docker volume rm piscine_data-science_postgres_data