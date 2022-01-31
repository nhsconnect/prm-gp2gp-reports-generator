# prm-gp2gp-reports-generator

This repo contains the reports generator. It is responsible for generating various reports needed for reporting on GP2GP. It will put the generated reports (such as the Error Combination table) into an S3 bucket.

## Configuration

Configuration is achieved via the following environment variables:


| Environment variable       | Description                                                             |
|----------------------------|-------------------------------------------------------------------------|
| INPUT_TRANSFER_DATA_BUCKET | Bucket to read transfer files from.                                     |
| OUTPUT_REPORTS_BUCKET      | Bucket to write the reports.                                            |
| BUILD_TAG                  | Unique identifier for version of code build tag (e.g. short git hash)   |

## Developing

Common development workflows are defined in the `tasks` script.

This project is written in Python 3.9.

### Recommended developer environment

- [pyenv](https://github.com/pyenv/pyenv) to easily switch Python versions.
- [Pipenv](https://pypi.org/project/pipenv/) to manage dependencies and virtual environments.
- [dojo](https://github.com/kudulab/dojo) and [Docker](https://www.docker.com/get-started)
  to run test suites in the same environment used by the CI/CD server.

#### Installing pyenv
```
brew install pyenv
```

#### Configure your shell's environment for Pyenv

```
For zsh:
echo 'eval "$(pyenv init --path)"' >> ~/.zprofile
echo 'eval "$(pyenv init -)"' >> ~/.zshrc
```

#### Install new python and set as default

```
pyenv install 3.9.6
pyenv global 3.9.6
```

#### Installing pipenv and updating pip

In a new shell, run the following:
```
python -m pip install pipenv
python -m pip install -U "pip>=21.1”
```

#### Build a dev env

In a new shell, in the project directory run.

```
./tasks devenv
```

This will create a python virtual environment containing all required dependencies.
To find out the path of this new virtual environment (which can be useful for configuring IDEs) run:
```
pipenv --venv
```

### Running the unit and integration tests

`./tasks test`

### Running the end to end tests

`./tasks e2e-test`

### Running tests, linting, and type checking

`./tasks validate`

### Running tests, linting, and type checking in a docker container

This will run the validation commands in the same container used by the GoCD pipeline.

`./tasks dojo-validate`

### Auto Formatting

`./tasks format`

### Dependency Scanning

`./tasks check-deps`

- If this fails when running outside of Dojo, see [troubleshooting section](### Troubleshooting)

### Troubleshooting

#### Checking dependencies fails locally due to pip

If running `./tasks check-deps` fails due to an outdated version of pip, yet works when running it in dojo (i.e. `./tasks dojo-deps`), then the local python environment containing pipenv may need to be updated (using pyenv instead of brew - to better control the pip version).
Ensure you have pyenv installed (use `brew install pyenv`).
Perform the following steps:

1. Run `brew uninstall pipenv`
2. Run the steps listed under [Installing correct version of pip and python](#installing-correct-version-of-pip-and-python)
3. Now running `./tasks check-deps` should pass.

#### Python virtual environments

If you see the below notice when trying to activate the python virtual environment, run `deactivate` before trying again.

> Courtesy Notice: Pipenv found itself running within a virtual environment, so it will automatically use that environment, instead of creating its own for any project. You can set PIPENV_IGNORE_VIRTUALENVS=1 to force pipenv to ignore that environment and create its own instead. You can set PIPENV_VERBOSITY=-1 to suppress this warning.
