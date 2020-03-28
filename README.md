# python-jenkins-sdlc
A Jenkins-based SDLC for quality control of Python code

## Getting Started
1. Download, install, and launch Docker https://www.docker.com
2. Open terminal -> `docker-compose build` -- this pulls down dependencies and builds the container
3. `docker-compose up` -- this launches the container locally at `https://localhost:8888

## Shell into container
To access the container's shell, do `docker-compose exec jupyter-service bash`.  All commands you run are now inside the container.  e.g.:

`flake8` - runs flake8 python linting on all files in the directory
`pytest` - runs pytest python unit testing on all files in the directory.  `pytest -v` for semi-detailed results, `pytest -vv` for most granular

