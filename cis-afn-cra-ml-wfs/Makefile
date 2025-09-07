launch_container:
	docker build -t my-dagster-app .
	docker run -p 3030:3030 my-dagster-app

start run:
	docker-compose up -d --build
	docker-compose logs -f --tail=40

stop:
	docker-compose down --volumes

logs:
	docker-compose logs -f --tail=40

pull:
	git pull && \
	cd ../cis-afn-cra-ml-api && git pull && \
	cd ../cis-afn-cra-ml-wfs && git pull && \

