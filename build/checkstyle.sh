#!/bin/bash

pipenv run pylint --rcfile ./pylintrc --output-format colorized $*
