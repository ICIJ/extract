clean:
		mvn clean

.PHONY: dist
dist:
		mvn validate package -Dmaven.test.skip=true

install:
		mvn install

help-db:
		mvn help:describe -DgroupId=org.liquibase -DartifactId=liquibase-maven-plugin -Dversion=2.0.1 -Dfull=true

release:
		mvn versions:set -DnewVersion=${NEW_VERSION}
		git commit -am "[release] ${NEW_VERSION} [skip ci]"
		git tag ${NEW_VERSION}
		echo "If everything is OK, you can push with tags i.e. git push origin master --tags"

unit:
		mvn test
