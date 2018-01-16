pip-install:
	pip install -r requirements-to-freeze.txt --upgrade

pip-freeze:
	pip freeze > requirements.txt
