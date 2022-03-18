import redis        # pip install redis
ip='34.130.141.192';
r = redis.Redis(host=ip, port=6379, db=0,password='SOFE4630U')
v=r.get('RandImages');

with open("./recieved.jpg", "wb") as f:
    f.write(v);
