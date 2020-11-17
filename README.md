# Instructions to run

**Requires:**
docker,
docker-compose

1. Build docker containers
```
docker-compose up
```
This creates a mongo database container and a python runner container.

2. Login to the python runner and run your desired function.
```
docker exec -it my_runner /bin/bash
my_runner:~ $ python3 manage_db.py
```

# Contributors

**Developers:**
[Royce Schultz](https://github.com/royceschultz),
[Andrew Yee](https://github.com/AndrewYeeYee),
[Chace Trevino](https://github.com/chacetrev10),
[Tianwei Zhao](https://github.com/ZTWHHH)

**Project Sponsor:**
[Diego Garcia](https://www.colorado.edu/business/leeds-directory/faculty/diego-garcia),
[CU Boulder FINLAB](http://leeds-faculty.colorado.edu/AsafBernstein/NLP_FIN_LAB.html)

# License

[MIT Open Source License](LICENSE.txt)
