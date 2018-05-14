
all:

clean:
	mvn clean

build: clean
	mvn package

sync: build
	rsync -az krv_cleanup krv_cleanup_loader list_tables target/restbase-krv-tools-1.0.0-SNAPSHOT-jar-with-dependencies.jar restbase-dev1004.eqiad.wmnet:
	rsync -az krv_cleanup krv_cleanup_loader list_tables target/restbase-krv-tools-1.0.0-SNAPSHOT-jar-with-dependencies.jar restbase1016.eqiad.wmnet:

.PHONY: all sync
