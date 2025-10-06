import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Lee la clave privada directamente desde un string multilínea
private_key_str = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCzHaaaSYqeAPfT
tmw6zesHokPoqzL7o6UN4s3Dhx8V2g0VVCw4eWxVr0K0cxvn0xu0OKsUI1vrA7G0
6XWagyJkGmpz+HzE3W4lDuakB+OoOh7CDNcHH9FbWyAsqKmunHgwClYheOf0otv4
Y46fU1wg6vN2SdCFzGNgDLOSh+Rvl+z1RPtSQLt5q4Rx0TTm8q9XN7tYWXNAKjwK
ZKRqUR17tZtaUDE/truQGeoOLnXWgqg0OLxzvxtmYJqG7x+0rEKEzCwz9CwZnROc
i8rg8ML6df0tAi2vTgfSJmPN1fbFPNKsie7pjAxJyF2Hboa84j3vgEmdwjfN14FA
H7hJnu4/AgMBAAECggEACYBMv1AGjsCvMYdltyDUYAB9gn2ovXk9+Ir++zY5sedx
9jjgatNJQ3cbUYZU0dFJXbmVaZx09ClEze65LEva3fbnJ37kFdVHhUCEsrX9OR+l
utY17QAFE2GCejWTYQ3JWlMFPc7m28hsmE2mDR5XSFYhbM7kxS3FJh0sxI/gA2mz
lYPxYGYOpSTazHqpgDw0OSiwGWGyhHxbVj0dOJiATleq0anji678nLBHx/TIlfVe
7xcmkKlGBzF3ZR7UmKCrQs7SsGkiJNNmP6Qxapd5wy42QGAhIK2HYUkqSHILgpWs
MQMyBNSDdiH/2yn1MgTTPxCKA1xt6XoRiQb6CdJkgQKBgQDze3v4kRoca5X/UtMM
zVP1ZcyhE7R353nc38tiJxvHfc4I+jSzqr+fdRv2zueNvMbr3C9yjFMiYAryfOXI
W8rWIWZKN2+poBw1/zf+d0Dcxg/TmBYLuYW73td9lMb3ua8KZEweeOx2/XrP6bo/
xd0uzf5l+DlbD8NsCa7K4O4SIQKBgQC8Uwbcb9X0F/0TcQIETd3nlmRS0W2zZukd
2Tz0bq0Q6jNdQc3fuwMqNY3lNXTCQQ9ak2InElnG+Xhe8hoQQF5afP0lSlLG50J8
1axS0hJQdlKs0qI0sYO4K3g6YJBmEIDw/56HpyLgIIMxZItWMf0Z63jL4hewGC9f
a11oeae0XwKBgGEQIxWuUqhtwzgrvnLmD8hOMssr3c/G+W+xz5RrXsmiP1aY2BWf
xhA9UU6MoQaB8RLpjgiuJB4aB4MvgzLiVPQUEIEZpGwMpfJosdvBkpvwYTLK+E7o
QIXqiiFIBCGRZlRQM4AaWLn+xszHsjXmHQyhlf70e3jvycnx+jpqfL3BAoGAd7lQ
41M18bhOa82sOpBGQrSZkw0RcLw7933kAoFaBSbfAKqU92cs2+iwDMevMs+psyt/
etdvu89ddv7zEuHZGi3bwZk+hrT+z94Hb5+dhQm0Bari9BzmYG9CP9qj8j8LuirH
3fWjdlk1DnGdI28kORY59WQHKyw08bSP7ZtpwoECgYEAn/3ZFXxwKQYDPAcY5wFB
6l5Jvi0NOr58gdRyF5F7u5YvCyfh5eMDeYVTJ8gF/MDe5ltH0YsIRQjNlPQM4iIn
f4poLaHwP9hE9lhFP/nNSgDJnxlga37sDdS+MvZISGCkfq2oi23ES17s3Ii2/kLy
uGYseec2zhVuBnLKenJtxIQ=
-----END PRIVATE KEY-----
"""

# Convertimos a objeto usable
p_key = serialization.load_pem_private_key(
    private_key_str.encode("utf-8"),
    password=None,  # o pon aquí tu passphrase si la clave tiene
    backend=default_backend()
)

# Serializamos a formato DER
private_key_bytes = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

# Conexión con Snowflake
conn = snowflake.connector.connect(
    user="MARTINASERVICEUSER",
    account="HFISOEI-XY68658",
    private_key=private_key_bytes,
    warehouse="COMPUTE_WH",
    database="NY_TAXI",
    schema="RAW",
    role="ROLE_INGESTA"
)

cur = conn.cursor()
cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
print(cur.fetchone())
