# Apache Airflow Tools

Shared tools for Apache Airflow

## Generating distribution archives

Update setuptools from pip:

```
python3 -m pip install --user --upgrade setuptools wheel
```

Generete setup:

```
python3 setup.py sdist bdist_wheel
```

Install Twine:

```
python3 -m pip install --user --upgrade twine
```

Upload files in [Pypi Test](https://test.pypi.org/account/register/).

```
python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

Install and Test:

```
python3 -m pip install --index-url https://test.pypi.org/simple/ swe-airflow-tools
```

Upload files in [Pypi](https://pypi.org/account/register/).

```
python3 -m twine upload dist/*
```

 Install from pip:

```
python3 -m pip install swe-airflow-tools
```
