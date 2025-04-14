SHELL	= /bin/sh

NAME	= piscine_data-science

all:
	docker compose up --build
postgres:
	docker exec -it postgres sh \
		-c "psql -U blarger -d piscineds -h localhost -W"
logs:
	docker-compose logs postgres