django-iris
===

InterSystems IRIS backend for [Django](https://www.djangoproject.com/)

Prerequisites
---

You must install, the latest version of InterSystems IRIS DB-API Python driver

```shell
pip3 install https://github.com/intersystems-community/intersystems-irispython/releases/download/3.7.3/intersystems_iris-3.7.3-py3-none-any.whl
```

Install and usage
---

Install with pip

`pip install django-iris`

Configure the Django `DATABASES` setting similar to this:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django_iris',
        'NAME': 'USER',
        'USER': '_SYSTEM',
        'PASSWORD': 'SYS',
        'HOST': 'localhost',
        'PORT': '1972',
    },
}
```
