SHELL	= /bin/sh

NAME	= piscine_data-science

all:
	docker compose up --build
logs:
	docker-compose logs postgres
down:
	docker-compose down
rm_volume:
	docker-compose down && \
	docker volume rm piscine_data-science_postgres_data
postgres:
	docker exec -it postgres /bin/bash
test_postgres:
	docker exec -it postgres sh \
		-c "psql -U blarger -d piscineds -h localhost -W"
restart_exec:
	docker-compose restart exec
customers_table:
	python3.10 ex01/customers_table.py
remove_dupliactes:
	python3.10 ex02/remove_duplicates.py
fusion:
	python3.10 ex03/fusion.py
