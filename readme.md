# Installing
To install follow instructions from https://pipenv.pypa.io/en/latest/

# Updating secrets
To run the AWS functions the .secrets.toml needs to be created and updated with the required secrets. An example secrets file is also provided in the repository.

# Requirements
- Python
- Java
- Neo4j

# Scripts
The following scripts are allowed to run:
```
main.py download single <page>
main.py download multiple (<pages>)
main.py upload all
main.py compare local
main.py compare cloud
main.py generate output
main.py generate graph
main.py analysis total revisions
```